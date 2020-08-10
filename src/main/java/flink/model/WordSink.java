package flink.model;

import lombok.Data;

import java.io.Serializable;

@Data
public class WordSink implements Serializable {
    private String word;
    private int count;
}
