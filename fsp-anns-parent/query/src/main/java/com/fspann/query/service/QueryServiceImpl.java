package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.crypto.ReencryptionTracker;
import com.fspann.loader.GroundtruthManager;
import com.fspann.query.core.QueryEvaluationResult;
import com.fspann.query.core.QueryTokenFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * QueryServiceImpl (Ideal System Version)
 * ---------------------------------------
 * - Partitioned-paper mode only (no LSH multiprobe)
 * - Supports K-adaptive query expansion
 * - Selective REENCRYPTION tracking (forward security)
 * - Dimension-safe L2
 */
public class QueryServiceImpl implements QueryService {
    private static final Logger logger = LoggerFactory.getLogger(QueryServiceImpl.class);
    private static final Pattern VERSION_PATTERN = Pattern.compile("epoch_(\\d+)_dim_(\\d+)$");

    private final IndexService indexService;
    private final CryptoService cryptoService;
    private final KeyLifeCycleService keyService;
    private final QueryTokenFactory tokenFactory;

    // Last-query metrics
    private volatile long lastQueryDurationNs = 0;
    private volatile int lastCandTotal = 0;
    private volatile int lastCandKeptVersion = 0;
    private volatile int lastCandDecrypted = 0;
    private volatile int lastReturned = 0;

    // Unfiltered candidate IDs (union, pre-version filter)
    private volatile List<String> lastCandIds = java.util.Collections.emptyList();
    private volatile ReencryptionTracker reencTracker;

    // Forward-security touch tracking (per service instance)
    private final Set<String> touchedThisSession = ConcurrentHashMap.newKeySet();
    private volatile int lastTouchedUniqueSoFar = 0;
    private volatile int lastTouchedCumulativeUnique = 0;

    public int getLastTouchedUniqueSoFar()      { return lastTouchedUniqueSoFar; }
    public int getLastTouchedCumulativeUnique() { return lastTouchedCumulativeUnique; }

    public QueryServiceImpl(IndexService indexService,
                            CryptoService cryptoService,
                            KeyLifeCycleService keyService,
                            QueryTokenFactory tokenFactory) {
        this.indexService   = Objects.requireNonNull(indexService);
        this.cryptoService  = Objects.requireNonNull(cryptoService);
        this.keyService     = Objects.requireNonNull(keyService);
        this.tokenFactory   = tokenFactory;
    }

    public void setReencryptionTracker(ReencryptionTracker tracker) {
        this.reencTracker = tracker;
    }

    /** FULL K-ADAPTIVE SEARCH ENTRY POINT **/
    @Override
    public List<QueryResult> search(QueryToken token) {
        if (token == null) return Collections.emptyList();
        clearLastMetrics();

        // Get key version
        KeyVersion kv;
        try {
            kv = keyService.getVersion(token.getVersion());
        } catch (Throwable t) {
            kv = keyService.getCurrentVersion();
        }

        // Decrypt query
        final double[] qVec = cryptoService.decryptQuery(
                token.getEncryptedQuery(),
                token.getIv(),
                kv.getKey()
        );

        final long tStart = System.nanoTime();
        lastCandIds = new ArrayList<>();

        try {
            // 1) INITIAL CANDIDATES FROM PARTITIONED INDEX
            List<EncryptedPoint> initialRaw;
            try {
                initialRaw = indexService.lookup(token);
            } catch (Throwable t) {
                logger.warn("Index lookup failed", t);
                initialRaw = Collections.emptyList();
            }

            final LinkedHashMap<String, EncryptedPoint> uniq = new LinkedHashMap<>();
            for (EncryptedPoint ep : initialRaw) {
                if (ep != null) {
                    uniq.putIfAbsent(ep.getId(), ep);
                }
            }
            addCandidateIds(uniq.keySet());
            lastCandTotal = uniq.size();

            List<QueryScored> scored = decryptAndScore(uniq, qVec, kv);

            // 2) SORT CURRENT CANDIDATES
            scored.sort(Comparator.comparingDouble(QueryScored::dist));

            // 3) ADAPTIVE K LOOP
            int requestedK = token.getTopK();
            List<QueryScored> finalScored =
                    kAdaptiveExpand(scored, uniq, token, qVec, kv);

            // final result: top-K from finalScored
            int kEff = Math.min(requestedK, finalScored.size());
            lastReturned = kEff;

            List<QueryResult> out = new ArrayList<>(kEff);
            for (int i = 0; i < kEff; i++) {
                QueryScored qs = finalScored.get(i);
                out.add(new QueryResult(qs.id(), qs.dist()));
            }

            // Safety: if somehow lastCandIds is still empty, fall back to uniq keys
            if (lastCandIds == null || lastCandIds.isEmpty()) {
                addCandidateIds(uniq.keySet());
            }

            return out;

        } finally {
            lastQueryDurationNs = Math.max(0L, System.nanoTime() - tStart);
        }
    }

    /** Decrypt + distance score + forward-security touch **/
    private List<QueryScored> decryptAndScore(Map<String, EncryptedPoint> uniq,
                                              double[] qVec,
                                              KeyVersion kv) {
        List<QueryScored> scored = new ArrayList<>(uniq.size());

        for (EncryptedPoint ep : uniq.values()) {
            if (ep.getVersion() != kv.getVersion()) continue;
            lastCandKeptVersion++;

            // selective re-encryption hit
            if (reencTracker != null) {
                reencTracker.touch(ep.getId());
            }
            touchedThisSession.add(ep.getId());
            addCandidateIds(Collections.singleton(ep.getId()));

            double[] v = cryptoService.decryptFromPoint(ep, kv.getKey());
            if (v == null || v.length == 0) continue;

            lastCandDecrypted++;
            scored.add(new QueryScored(ep.getId(), l2sq(qVec, v)));
        }

        // Update touch metrics
        lastTouchedUniqueSoFar      = uniq.size();
        lastTouchedCumulativeUnique = touchedThisSession.size();
        return scored;
    }

    @Override
    public List<QueryEvaluationResult> searchWithTopKVariants(QueryToken baseToken,
                                                              int queryIndex,
                                                              GroundtruthManager gt) {
        Objects.requireNonNull(baseToken, "baseToken");

        final List<Integer> topKVariants = List.of(1, 5, 10, 20, 40, 60, 80, 100);
        final long clientStartNs = System.nanoTime();
        final List<QueryResult> baseResults = nn(search(baseToken));
        final long clientEndNs   = System.nanoTime();

        final long clientWindowNs   = Math.max(0L, clientEndNs - clientStartNs);
        final long serverNsBounded  = getLastQueryDurationNsCappedTo(clientWindowNs);
        final long queryDurationMs  = Math.round(serverNsBounded / 1_000_000.0);

        final int candTotal       = getLastCandTotal();
        final int candKeptVersion = getLastCandKeptVersion();
        final int candDecrypted   = getLastCandDecrypted();
        final int returned        = getLastReturned();

        final List<QueryEvaluationResult> out = new ArrayList<>(topKVariants.size());
        for (int k : topKVariants) {
            final int upto = Math.min(k, baseResults.size());
            final List<QueryResult> prefix = baseResults.subList(0, upto);

            // Precision@K
            int[] gtArr = (gt != null) ? safeGt(gt.getGroundtruth(queryIndex, k)) : new int[0];
            double precision = 0.0;
            if (k > 0 && gtArr.length > 0 && upto > 0) {
                final Set<String> truthSet = Arrays.stream(gtArr)
                        .mapToObj(String::valueOf)
                        .collect(Collectors.toSet());
                int hits = 0;
                for (int i = 0; i < upto; i++) {
                    if (truthSet.contains(prefix.get(i).getId())) hits++;
                }
                precision = ((double) hits) / (double) k;
            }

            out.add(new QueryEvaluationResult(
                    k,                  // topKRequested
                    upto,               // retrieved
                    Double.NaN,         // ratio (computed elsewhere)
                    precision,          // precision
                    queryDurationMs,    // timeMs (server)
                    0,                  // insertTimeMs
                    candDecrypted,      // candDecrypted
                    0,                  // tokenSizeBytes
                    0,                  // vectorDim
                    0,                  // totalFlushedPoints
                    0,                  // flushThreshold
                    0,                  // touchedCount
                    0,                  // reencryptedCount
                    0,                  // reencTimeMs
                    0,                  // reencBytesDelta
                    0,                  // reencBytesAfter
                    "test",             // ratioDenomSource
                    0,                  // clientTimeMs
                    k,                  // tokenK
                    k,                  // tokenKBase
                    0,                  // qIndexZeroBased
                    "test"              // candMetricsMode
            ));
        }
        return out;
    }

    // ------------------------- K-ADAPTIVE CORE -------------------------

    private List<QueryScored> kAdaptiveExpand(List<QueryScored> scored,
                                              LinkedHashMap<String, EncryptedPoint> uniq,
                                              QueryToken token,
                                              double[] qVec,
                                              KeyVersion kv) {
        boolean enabled = Boolean.getBoolean("kadaptive.enabled");
        if (!enabled) return scored;

        int minCandidates = Integer.getInteger("kadaptive.minCandidates", 128);
        int maxCandidates = Integer.getInteger("kadaptive.maxCandidates", 8192);
        int batch         = Integer.getInteger("kadaptive.batchSize", 128);

        double lowReturnThresh = Double.parseDouble(
                System.getProperty("kadaptive.lowReturnRateThreshold", "0.5")
        );
        double boostFactor = Double.parseDouble(
                System.getProperty("kadaptive.boostFactorOnLowReturn", "2.0")
        );

        int requestedK = token.getTopK();

        while (true) {
            // compute returnRate
            double returnRate = (scored.isEmpty() ? 0.0 :
                    Math.min(1.0,
                            ((double) requestedK) /
                                    (double) Math.max(1, scored.size())
                    ));

            // stop if high returnRate
            if (returnRate >= lowReturnThresh) {
                logger.debug("K-adaptive: stopping – returnRate={}", returnRate);
                break;
            }

            // need more candidates
            int need = (int) (scored.size() * boostFactor);
            if (need < minCandidates) need = minCandidates;
            if (need > maxCandidates) need = maxCandidates;
            if (need <= scored.size()) break;

            // fetch more candidates (paper engine union semantics)
            List<EncryptedPoint> more = fetchMoreCandidates(token, uniq, need);
            if (more.isEmpty()) break;
            for (EncryptedPoint ep : more) {
                if (ep != null) addCandidateIds(Collections.singletonList(ep.getId()));
            }

            // Keep lastCandIds consistent with uniq
            addCandidateIds(uniq.keySet());
            lastCandTotal = uniq.size();

            // decrypt & add new scores
            List<QueryScored> moreScored = decryptAndScore(uniq, qVec, kv);
            scored.addAll(moreScored);
            scored.sort(Comparator.comparingDouble(QueryScored::dist));

            if (scored.size() >= maxCandidates) break;
        }

        return scored;
    }

    private List<EncryptedPoint> fetchMoreCandidates(QueryToken tok,
                                                     LinkedHashMap<String, EncryptedPoint> uniq,
                                                     int target) {
        try {
            List<EncryptedPoint> extra = indexService.lookup(tok);
            int before = uniq.size();
            for (EncryptedPoint ep : extra)
                if (ep != null) uniq.putIfAbsent(ep.getId(), ep);
            return extra.subList(before, uniq.size());
        } catch (Throwable t) {
            return Collections.emptyList();
        }
    }

    // ------------------------- Utility -------------------------

    private record QueryScored(String id, double dist) {}

    private static <T> List<T> nn(List<T> v) { return (v == null) ? Collections.emptyList() : v; }
    private static int[] safeGt(int[] a)     { return (a == null) ? new int[0] : a; }

    private double l2sq(double[] a, double[] b) {
        int len = Math.min(a.length, b.length);
        double s = 0.0;
        for (int i = 0; i < len; i++) {
            double d = a[i] - b[i];
            s += d * d;
        }
        return s;
    }

    public static int estimateTokenSizeBytes(QueryToken t) {
        int bytes = 0;
        if (t.getIv() != null) bytes += t.getIv().length;
        if (t.getEncryptedQuery() != null) bytes += t.getEncryptedQuery().length;
        int bucketCount = 0;
        for (List<Integer> l : t.getTableBuckets()) bucketCount += l.size();
        bytes += bucketCount * Integer.BYTES;

        // approximate BitSet footprint
        BitSet[] codes = t.getCodes();
        if (codes != null) {
            for (BitSet bs : codes) {
                if (bs != null) {
                    int bits = Math.max(0, bs.length());
                    bytes += (bits + 7) / 8;
                }
            }
        }
        return bytes;
    }

    private void clearLastMetrics() {
        lastQueryDurationNs = 0L;
        lastCandTotal       = 0;
        lastCandKeptVersion = 0;
        lastCandDecrypted   = 0;
        lastReturned        = 0;
    }

    /**
     * Centralized safe updater for lastCandIds.
     * Ensures no overwrites, no empty states, and preserves insertion order.
     */
    private void addCandidateIds(Collection<String> ids) {
        if (ids == null || ids.isEmpty()) return;
        if (lastCandIds == null || lastCandIds.isEmpty()) {
            lastCandIds = new ArrayList<>(ids);
            return;
        }
        // Merge without duplicates
        LinkedHashSet<String> merged = new LinkedHashSet<>(lastCandIds);
        merged.addAll(ids);
        lastCandIds = new ArrayList<>(merged);
    }

    public long getLastQueryDurationNsCappedTo(long clientWindowNs) {
        if (clientWindowNs <= 0L) return Math.max(0L, lastQueryDurationNs);
        return Math.min(Math.max(0L, lastQueryDurationNs), clientWindowNs);
    }

    public List<String> getLastCandidateIds() {
        return (lastCandIds == null)
                ? java.util.Collections.emptyList()
                : java.util.Collections.unmodifiableList(lastCandIds);
    }

    public long getLastQueryDurationNs() { return lastQueryDurationNs; }
    public int  getLastCandTotal()       { return lastCandTotal; }
    public int  getLastCandKeptVersion() { return lastCandKeptVersion; }
    public int  getLastCandDecrypted()   { return lastCandDecrypted; }
    public int  getLastReturned()        { return lastReturned; }
}
