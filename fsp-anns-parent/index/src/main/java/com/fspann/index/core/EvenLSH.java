package com.fspann.index.core;

import java.security.SecureRandom;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EvenLSH: cosine projection + dynamic quantiles.
 */
public class EvenLSH {
    private static final Logger log = LoggerFactory.getLogger(EvenLSH.class);
    private final int dimensions;
    private double[] proj;
    private double[] thresholds;

    public EvenLSH(int dimensions, int numBuckets){
        this.dimensions = dimensions;
        this.proj = randomUnit(dimensions);
        this.thresholds = new double[numBuckets-1];
    }

    public double project(double[] v){ double s=0;for(int i=0;i<v.length;i++)s+=v[i]*proj[i];return s; }
    public int bucket(double[] v){ double p=project(v); for(int i=0;i<thresholds.length;i++) if(p<=thresholds[i]) return i+1; return thresholds.length+1; }

    public void updateThresholds(List<double[]> data){
        List<Double> vals=new ArrayList<>();for(double[]v:data)vals.add(project(v));
        Collections.sort(vals);
        int m=thresholds.length+1;
        for(int i=1;i<=thresholds.length;i++){
            int idx=(int)Math.floor(i*vals.size()/(double)(m));
            thresholds[i-1]=vals.get(Math.min(idx,vals.size()-1));
        }
        log.debug("Thresholds updated: {}", Arrays.toString(thresholds));
    }

    private static double[] randomUnit(int d){ SecureRandom r=new SecureRandom(); double[] v=new double[d]; double norm=0; for(int i=0;i<d;i++){v[i]=r.nextGaussian();norm+=v[i]*v[i];} norm=Math.sqrt(norm); for(int i=0;i<d;i++)v[i]/=norm; return v; }
}