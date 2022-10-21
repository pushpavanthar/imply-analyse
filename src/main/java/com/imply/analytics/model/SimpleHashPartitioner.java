package com.imply.analytics.model;

import lombok.Data;

@Data
public class SimpleHashPartitioner implements IPartitioner<String> {
    private Integer partitionCount;

    public SimpleHashPartitioner(Integer partitionCount){
        this.partitionCount = partitionCount;
    }

    @Override
    public Integer partition(String line) {
        String[] splits = line.split(",");
        return splits[1].hashCode()%partitionCount;
    }
}
