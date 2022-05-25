package org.example;

import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.text.sentenceiterator.BasicLineIterator;
import org.deeplearning4j.text.sentenceiterator.SentenceIterator;
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor;
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory;
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class Analysis {
    public static void wordToVec (String filename) throws IOException {
        SentenceIterator iter = new BasicLineIterator(filename);

        TokenizerFactory t = new DefaultTokenizerFactory();
        t.setTokenPreProcessor(new CommonPreprocessor());

        Word2Vec vec = new Word2Vec.Builder()
                .minWordFrequency(2)
                .layerSize(100)
                .seed(42)
                .windowSize(5)
                .iterate(iter)
                .tokenizerFactory(t)
                .build();
        System.out.println("Fitting Word2Vec model....");
        vec.fit();

        WordVectorSerializer.writeWordVectors(vec, filename + "vector");
    }

    public static void nearDuplicates(String filename1, String filename2) throws IOException {
        wordToVec(filename1);
        wordToVec(filename2);

        Set<String> set1 = new HashSet<>();
        Set<String> set2 = new HashSet<>();

        FileReader fr1 = new FileReader(filename1 + "vector");
        extractWordsToSet(set1, fr1);

        FileReader fr2 = new FileReader(filename2 + "vector");
        extractWordsToSet(set2, fr2);

        MinHash<String> f = new MinHash<>( set1.size() + set2.size());
        System.out.println(set1.size() + set2.size());
        System.out.println("Similarity between two sets: " + f.similarity(set1, set2));
    }


    public static void createFile(String filename, String text) throws IOException {
        String form_text = text.toLowerCase(Locale.ROOT)
                .replaceAll("-", "")
                .replaceAll("–", "")
                .replaceAll("—", "")
                .replaceAll("%", "")
                .replaceAll("№", "")
                .replaceAll("\\.\\.\\.", ".")
                .replaceAll("\\.\\.", ".")
                .replaceAll("\\?.\\..", "?")
                .replaceAll("[0-9]", "")
                .replaceAll(",", "")
                .replaceAll("\\s.{1,3}\\.", "")
                .replaceAll("[a-zA-Z]", "")
                .replaceAll("\\b.{2,4}[А-Я]\\b", "")
                .replaceAll("\\b.{1,3}[a-я]\\b", "")
                .replaceAll("\\.", " .\n")
                .replaceAll("\\?.", " ?\n")
                .replaceAll("!", " !\n")
                .replaceAll("…", " .\n")
                .replaceAll("«", "")
                .replaceAll("»", "")
                .replaceAll("\\(", "")
                .replaceAll("\\)", "")
                .replaceAll(":", "")
                .replaceAll("\"", "")
                .replaceAll("\\+", "");
        Files.writeString(Path.of(filename), form_text, StandardCharsets.UTF_8);
    }

    public static void extractWordsToSet(Set<String> set, FileReader fr) throws IOException {
        BufferedReader b = new BufferedReader(fr);
        String line;
        String[] words = null;
        while((line = b.readLine()) != null) {
            words = line.replaceAll("[.\\- [0-9]]", "").replaceAll("E","").split("\\b", 0);
            set.addAll(Arrays.asList(words));
        }
        fr.close();
        b.close();
        set.addAll(Arrays.asList(words));
    }
}