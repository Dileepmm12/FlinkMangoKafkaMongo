package com.example.demo.dto;

public class WordCount {
    private String word;
    private long count;

    public WordCount() {}

    public WordCount(String word, long count) {
        this.word = word;
        this.count = count;
    }

    // Getters and setters
    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "WordCount{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}