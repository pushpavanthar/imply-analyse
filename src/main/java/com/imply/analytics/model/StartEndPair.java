package com.imply.analytics.model;

public class StartEndPair {
    public long start;
    public long end;

    @Override
    public String toString() {
        return "star="+start+";end="+end;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (end ^ (end >>> 32));
        result = prime * result + (int) (start ^ (start >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        StartEndPair other = (StartEndPair) obj;
        if (end != other.end)
            return false;
        if (start != other.start)
            return false;
        return true;
    }

}
