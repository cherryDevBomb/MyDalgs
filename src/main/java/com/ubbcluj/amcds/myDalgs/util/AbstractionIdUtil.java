package com.ubbcluj.amcds.myDalgs.util;

import com.ubbcluj.amcds.myDalgs.model.AbstractionType;

public class AbstractionIdUtil {

    public static final String HUB_ID = "hub";

    public static String getChildAbstractionId(String parentAbstractionId, AbstractionType childAbstractionType) {
        return parentAbstractionId + "." + childAbstractionType.getId();
    }

    public static String getParentAbstractionId(String childAbstractionId) {
        return childAbstractionId.substring(0, childAbstractionId.lastIndexOf("."));
    }

    public static String getNamedAbstractionId(String parentAbstractionId, AbstractionType abstractionType, String name) {
        return getChildAbstractionId(parentAbstractionId, abstractionType) + "[" + name + "]";
    }

    public static String getInternalNameFromAbstractionId(String abstractionId) {
        return abstractionId.substring(abstractionId.indexOf("[") + 1, abstractionId.indexOf("]"));
    }
}
