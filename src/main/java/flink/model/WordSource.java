package flink.model;

import lombok.Data;

import java.io.Serializable;

@Data
public class WordSource implements Serializable {
    private String word;
}
