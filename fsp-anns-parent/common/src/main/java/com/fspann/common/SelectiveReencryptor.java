package com.fspann.common;

import java.util.Collection;

public interface SelectiveReencryptor {
    ReencryptReport reencryptTouched(Collection<String> touchedIds, int targetVersion, StorageSizer sizer);
}
