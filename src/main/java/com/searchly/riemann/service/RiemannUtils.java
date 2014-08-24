package com.searchly.riemann.service;

/**
 * @author ferhat
 */
public class RiemannUtils {

    public static String getState(long param, long ok, long warning) {
        if (param < ok) {
            return "ok";
        } else if (param < warning) {
            return "warning";
        }
        return "critical";
    }

    public static String getStateWithClusterInformation(String clusterState) {

        if (clusterState == null) {
            return "critical";
        }

        if (clusterState.equalsIgnoreCase("green")) {
            return "ok";
        }
        if (clusterState.equalsIgnoreCase("yellow")) {
            return "warning";
        }

        return "critical";
    }
}
