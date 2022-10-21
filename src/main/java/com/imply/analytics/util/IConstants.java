package com.imply.analytics.util;

public interface IConstants {
    String DUMMY =  "DUMMY";
    String WORKING_DIR = "workingDir";
    String FILE_SEPARATOR  = "/";
    String FILE_SUFFIX  = ".txt";
    // directory for writing raw data into multiple files based on hash partition
    String MAP0_LOCATION = WORKING_DIR + FILE_SEPARATOR + "map0";
    // directory for writing userid to distinct page paths per partition file
    String MAP1_LOCATION = WORKING_DIR + FILE_SEPARATOR + "map1";
    // location of filtered userIDs
    String OUTPUT_FILE_LOCATION = WORKING_DIR + FILE_SEPARATOR + "output";
}
