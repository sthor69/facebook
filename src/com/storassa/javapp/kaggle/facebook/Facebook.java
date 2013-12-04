package com.storassa.javapp.kaggle.facebook;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Facebook {

    private String title = "";
    private String body = "";
    private ArrayList<String> tags;

    // symbol tables for title/body and for tags
    private HashMap<String, Integer> uniqueWordsTotal;
    private HashMap<String, Integer> tagsTotal;

    // map to count the tag recurrence in the training set
    private HashMap<Integer, Integer> postPerTag;

    private int wordsTotal;
    private int incWordsTotal;
    private int numberOfFiles = 0;
    private int postTotal = 0;
    private int longestTitleBody;
    private int maxPosts;

    public Facebook(int _maxPosts) {
        uniqueWordsTotal = new HashMap<String, Integer>(7000000);
        tagsTotal = new HashMap<String, Integer>(60000);
        wordsTotal = 0;
        postPerTag = new HashMap<Integer, Integer>(60000);
        maxPosts = _maxPosts;
    }

    public void createSymbolTable(String filename) {

        long start = System.currentTimeMillis();

        BufferedReader in;

        try {
            boolean finished = false;
            int count = 1;
            int wordCount = 0, tagCount = 0;

            in = new BufferedReader(new FileReader(filename));

            // skip the header row
            String line = in.readLine();

            // for (int t = 0; t < 1; t++) {
            // line = in.readLine();
            while ((line = in.readLine()) != null) {
                postTotal++;
                if (postTotal > maxPosts * 1000)
                    break;
                body = "";

                // get the title, the body and the tags
                line = line.replace("\"\"", "").replace("\'", "");
                finished = process(line, State.ID);
                while (!finished) {
                    if ((line = in.readLine()) != null) {
                        line = line.replace("\"\"", "").replace("\'", "");
                        finished = process(line, State.BODY);
                    } else
                        finished = true;
                }

                // add all the words in the title in the symbol table
                ArrayList<String> titleList = getString(title);
                for (String s : titleList)
                    uniqueWordsTotal.put(s, wordCount++);

                // add all the words in the body in the symbol table
                ArrayList<String> bodyList = getString(body);
                for (String s : bodyList)
                    uniqueWordsTotal.put(s, wordCount++);

                // update the maximum body + title size
                if (bodyList.size() + titleList.size() > longestTitleBody)
                    longestTitleBody = bodyList.size() + titleList.size();

                for (String s : tags) {
                    // fill in the tags symbol table
                    if (!tagsTotal.containsKey(s))
                        tagsTotal.put(s, tagCount++);
                }

                // debug output
                printStatistics(count++);

            }

            // write the symbol table in a file
            BufferedWriter out = new BufferedWriter(new FileWriter(OUTPUT_DIR
                    + "wordsSymbolTable.txt"), BUFFER_SIZE);
            out.append(uniqueWordsTotal.keySet().size() + "\n");
            for (String s : uniqueWordsTotal.keySet())
                out.append(s + "," + uniqueWordsTotal.get(s) + "\n");
            out.close();

            // write the tag table in a file
            out = new BufferedWriter(new FileWriter(OUTPUT_DIR
                    + "tagsSymbolTable.txt"), BUFFER_SIZE);
            out.append(tagsTotal.keySet().size() + "\n");
            for (String s : tagsTotal.keySet())
                out.append(s + "," + tagsTotal.get(s) + "\n");
            out.close();

            // debug prints
            System.out.println("Symbol tables creation time: "
                    + (System.currentTimeMillis() - start) / 1000 + " seconds");
            System.out.println("Total number of words (also repeated): "
                    + uniqueWordsTotal.size());
            System.out
                    .println("Longest title+body length: " + longestTitleBody);

            in.close();

        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    private void printStatistics(int c) {
        if (c % POST_PER_FILE == 0) {
            System.out.println(c / POST_PER_FILE + ": "
                    + uniqueWordsTotal.size() + " TUW, " + wordsTotal + " TW, "
                    + uniqueWordsTotal.size() / c + " UWPP, " + wordsTotal / c
                    + " TWPP, " + incWordsTotal / 1000 + " ITWPP");
            incWordsTotal = 0;
        }
    }

    public void createTagRecurrenceMap(int numberOfFiles) {
        DataInputStream in;
        try {
            for (int i = 0; i < numberOfFiles; i++) {
                in = new DataInputStream(new BufferedInputStream(
                        new FileInputStream(OUTPUT_DIR + "out" + i + ".txt")));
                while (in.available() > 0) {
                    skipToTags(in);
                    int tagNr = in.readInt();
                    for (int t = 0; t < tagNr; t++) {
                        int tag = in.readInt();
                        if (postPerTag.isEmpty() | postPerTag.get(tag) == null)
                            postPerTag.put(tag, 1);
                        else
                            postPerTag.put(tag, postPerTag.get(tag) + 1);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    private void skipToTags(DataInputStream in) {
        try {
            in.readInt();
            int max = in.readInt();
            for (int i = 0; i < max; i++)
                in.readInt();
            max = in.readInt();
            for (int i = 0; i < max; i++)
                in.readInt();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public void writeOutputFiles(String filename) {
        int count = 0;
        long start = System.currentTimeMillis();

        try {
            BufferedReader in = new BufferedReader(new FileReader(filename));
            // delete the first row "Id","Title","Body","Tags"
            in.readLine();
            DataOutputStream out = new DataOutputStream(
                    new BufferedOutputStream(new FileOutputStream(OUTPUT_DIR
                            + "out" + count / POST_PER_FILE + ".txt")));

            String line;
            boolean finished;
            // for (int t = 0; t < 1; t++) {
            // line = in.readLine();
            while ((line = in.readLine()) != null) {
                body = "";
                if (count >= maxPosts * 1000)
                    break;

                if (count % POST_PER_FILE == 0) {
                    numberOfFiles++;
                    if (count > 0) {
                        out.flush();
                        out.close();
                        out = new DataOutputStream(new BufferedOutputStream(
                                new FileOutputStream(OUTPUT_DIR + "out" + count
                                        / POST_PER_FILE + ".txt"), BUFFER_SIZE));
                        System.out.println(count / POST_PER_FILE);
                    }
                }
                count++;

                // get the title, body and tags of the current post
                line = line.replace("\"\"", "").replace("\'", "");
                finished = process(line, State.ID);
                while (!finished) {
                    line = in.readLine().replace("\"\"", "").replace("\'", "");
                    finished = process(line, State.BODY);
                }

                ArrayList<String> titleList = getString(title);
                ArrayList<String> bodyList = getString(body);

                // write the current post id
                out.writeInt(count);

                // write the int reference for the words in the title
                out.writeInt(titleList.size());
                for (String s : titleList)
                    out.writeInt(uniqueWordsTotal.get(s));

                // write the int reference for the words in the body
                out.writeInt(bodyList.size());
                for (String s : bodyList) {
                    // debugging check to verify that we are inserting correct
                    // words
                    if (uniqueWordsTotal.containsKey(s))
                        out.writeInt(uniqueWordsTotal.get(s));
                    else
                        System.out.println(s);
                }

                // write the int reference for the tags
                out.writeInt(tags.size());
                for (String s : tags) {
                    int tag = tagsTotal.get(s);
                    out.writeInt(tag);
                }

                // every POST_PER_FILE posts open a new file and close the
                // previous
                // one

            }

            // debug prints
            System.out.println();
            System.out.println("Number of tags: " + tagsTotal.size());
            System.out.println("Number of words linked to specific tags: "
                    + uniqueWordsTotal.size());
            System.out.println("Output files writing time: "
                    + (System.currentTimeMillis() - start) / 1000 + " seconds");

            in.close();
            out.close();
        } catch (FileNotFoundException e) {
            System.out.println("Error opening the file");
            e.printStackTrace();
            System.exit(0);
        } catch (IOException e) {
            System.out.println("Error reading the file");
            e.printStackTrace();
            System.exit(0);
        }
    }

    public void train(String filename) {

    }

    public void removeCommonWords() {
        HashSet<Integer> uselessWordList = createUselessWordList();

        removeUselessWords(uselessWordList);
    }

    public void removeUncommonTags(double minPostPerTag, int numberOfFiles) {
        HashSet<Integer> removedTags = new HashSet<Integer>();

        createTagRecurrenceMap(numberOfFiles);
        HashSet<Integer> list = createTagList(numberOfFiles);
        // add in the removedTags set the tags that are present in a small
        // fraction of total posts
        for (Integer s : list)
            if (postPerTag.get(s) < postTotal * minPostPerTag)
                removedTags.add(s);

        try {
            int id, titleWords, bodyWords, title, body, tags;
            LinkedList<Integer> tagList = new LinkedList<Integer>();
            int maxBodySize = 0;
            int maxTitleSize = 0;

            for (int i = 0; i < numberOfFiles; i++) {
                DataInputStream in = new DataInputStream(
                        new BufferedInputStream(new FileInputStream(OUTPUT_DIR
                                + "out" + i + ".txt")));
                DataOutputStream out = new DataOutputStream(
                        new BufferedOutputStream(new FileOutputStream(
                                OUTPUT_DIR + "/removedTags/outCommonTags" + i
                                        + ".txt")));
                BufferedWriter xCsv = new BufferedWriter(new FileWriter(
                        OUTPUT_DIR + "/removedTags/outCommonTags" + i + ".csv"));

                BufferedWriter yCsv = new BufferedWriter(new FileWriter(
                        OUTPUT_DIR + "/removedTags/y" + i + ".csv"));

                while (in.available() > 0) {
                    // skip id
                    id = in.readInt();
                    out.writeInt(id);
                    xCsv.append(String.valueOf(id));
                    yCsv.append(String.valueOf(id));

                    // skip title
                    titleWords = in.readInt();
                    if (titleWords > maxTitleSize) {
                        System.out.println("New max for title: " + titleWords);
                        maxTitleSize = titleWords;
                    }
                    out.writeInt(titleWords);
                    for (int t = 0; t < titleWords; t++) {
                        title = in.readInt();
                        out.writeInt(title);
                        xCsv.append("," + title);
                    }

                    // skip body
                    bodyWords = in.readInt();
                    if (bodyWords > maxBodySize) {
                        System.out.println("New max for body: " + bodyWords);
                        maxBodySize = bodyWords;
                    }
                    out.writeInt(bodyWords);
                    for (int t = 0; t < bodyWords; t++) {
                        body = in.readInt();
                        out.writeInt(body);
                        xCsv.append("," + body);
                    }

                    // get all tags
                    tags = in.readInt();
                    for (int t = 0; t < tags; t++) {
                        Integer temp = in.readInt();
                        if (!removedTags.contains(temp))
                            tagList.add(temp);
                    }
                    xCsv.append("\n");

                    // write all tags
                    out.writeInt(tagList.size());
                    for (int t : tagList) {
                        out.writeInt(t);
                        yCsv.append("," + t);
                    }
                    yCsv.append("\n");
                    tagList.clear();
                }

                in.close();
                out.close();
                xCsv.close();
                yCsv.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    private HashSet<Integer> createTagList(int numberOfFiles) {
        HashSet<Integer> result = new HashSet<Integer>();
        int count = 0;
        try {
            for (int i = 0; i < numberOfFiles; i++) {
                DataInputStream in = new DataInputStream(
                        new BufferedInputStream(new FileInputStream(OUTPUT_DIR
                                + "out" + i + ".txt")));
                while (in.available() > 0) {
                    count++;
                    skipToTags(in);
                    int tagNr = in.readInt();
                    for (int t = 0; t < tagNr; t++) {
                        result.add(in.readInt());
                    }
                }
                in.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        postTotal = count;
        return result;
    }

    private void removeUselessWords(HashSet<Integer> uselessWords) {
        DataInputStream dis;
        DataOutputStream dos;

        ArrayList<Integer> titleWords;
        ArrayList<Integer> bodyWords;
        ArrayList<Integer> tagWords;

        // read title, body and tag words
        try {
            for (int i = 0; i < numberOfFiles; i++) {
                dis = new DataInputStream(new BufferedInputStream(
                        new FileInputStream(OUTPUT_DIR + "out" + i + ".txt")));

                // skip the id number
                dis.readInt();

                // keep all the words in the title
                titleWords = new ArrayList<Integer>();
                int title = dis.readInt();
                for (int t = 0; t < title; t++) {
                    titleWords.add(dis.readInt());
                }

                // keep only useful words in the body
                bodyWords = new ArrayList<Integer>();
                int body = dis.readInt();
                for (int t = 0; t < body; t++) {
                    int w = dis.readInt();
                    if (!uselessWords.contains(w))
                        bodyWords.add(w);
                }

                // keep all the tags
                tagWords = new ArrayList<Integer>();
                int tags = dis.readInt();
                for (int t = 0; t < tags; t++)
                    tagWords.add(dis.readInt());

                dis.close();

                // re-write the file deleting the useless words
                dos = new DataOutputStream(new BufferedOutputStream(
                        new FileOutputStream(OUTPUT_DIR + "out" + i + ".txt")));

                dos.writeInt(i);

                dos.writeInt(titleWords.size());
                for (int t = 0; t < titleWords.size(); t++)
                    dos.writeInt(titleWords.get(t));

                dos.writeInt(bodyWords.size());
                for (int t = 0; t < bodyWords.size(); t++)
                    dos.writeInt(bodyWords.get(t));

                dos.writeInt(tagWords.size());
                for (int t = 0; t < tagWords.size(); t++)
                    dos.writeInt(tagWords.get(t));
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    private HashSet<Integer> createUselessWordList() {

        HashSet<Integer> result = new HashSet<Integer>();
        HashMap<Integer, HashSet<Integer>> words2Tag = createWords2TagMap();

        for (int w : words2Tag.keySet())
            if (words2Tag.get(w).size() > tags.size() * PERC_THR_TAGS)
                result.add(w);

        return result;
    }

    private HashMap<Integer, HashSet<Integer>> createWords2TagMap() {
        HashMap<Integer, HashSet<Integer>> wordsToTagsMap = new HashMap<Integer, HashSet<Integer>>();

        DataInputStream in;
        try {
            for (int i = 0; i < numberOfFiles; i++) {
                in = new DataInputStream(new BufferedInputStream(
                        new FileInputStream(OUTPUT_DIR + "out" + i + ".txt")));
                in.readInt();

                // skip the title
                int numberWords = in.readInt();
                for (int t = 0; t < numberWords; t++)
                    in.readInt();

                // read the body words
                numberWords = in.readInt();
                int[] words = new int[numberWords];
                for (int t = 0; t < numberWords; t++)
                    words[t] = in.readInt();

                // read the tags
                int numberTags = in.readInt();
                ArrayList<Integer> tags = new ArrayList<Integer>();
                for (int t = 0; t < numberTags; t++)
                    tags.add(in.readInt());

                // fill in the map
                for (int t : words)
                    wordsToTagsMap.put(t, new HashSet<Integer>(tags));

                in.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        return wordsToTagsMap;
    }

    public void writeCsvTagFile() {
        try {
            BufferedReader in = new BufferedReader(new FileReader(OUTPUT_DIR
                    + "tagsSymbolTable.txt"), BUFFER_SIZE);
            BufferedWriter out = new BufferedWriter(new FileWriter(OUTPUT_DIR
                    + "tagsSymbolTable.csv"), BUFFER_SIZE);

            int count = 0;
            String line;
            String numTagsString = in.readLine();
            int numTags = Integer.parseInt(numTagsString);
            int[] tags = new int[numTags];
            while ((line = in.readLine()) != null) {
                tags[count++] = Integer.parseInt(line.split(",")[1]);
            }
            Arrays.sort(tags);
            for (int i = 0; i < tags.length; i++)
                out.append(tags[i] + "\n");

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public void writeY(int maxTag) {

        int id, tmp;

        try {
            for (int i = 0; i < 603; i++) {
                DataInputStream in = new DataInputStream(
                        new BufferedInputStream(new FileInputStream(OUTPUT_DIR
                                + "out" + i + ".txt"), BUFFER_SIZE));
                BufferedWriter out = new BufferedWriter(new FileWriter(
                        OUTPUT_DIR + "Y" + i + "Train.csv"), BUFFER_SIZE);

                writeTagsInY(in, out, maxTag);
            }
            // in = new DataInputStream(new BufferedInputStream(
            // new FileInputStream(OUTPUT_DIR + "out1.txt"), BUFFER_SIZE));
            // out = new BufferedWriter(new FileWriter(OUTPUT_DIR + "Ycv.csv"),
            // BUFFER_SIZE);
            //
            // writeTagsInY(in, out, maxTag);
            //
            // in = new DataInputStream(new BufferedInputStream(
            // new FileInputStream(OUTPUT_DIR + "out2.txt"), BUFFER_SIZE));
            // out = new BufferedWriter(new FileWriter(OUTPUT_DIR +
            // "Ytest.csv"),
            // BUFFER_SIZE);
            //
            // writeTagsInY(in, out, maxTag);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

    }

    private void writeTagsInY(DataInputStream _in, BufferedWriter _out,
            int _maxTag) {
        int numTitle, numBody, numTags;
        int id, tmp;
        try {
            while (_in.available() > 0) {
                // add id;
                _out.append(_in.readInt() + ",");

                // skip title
                numTitle = _in.readInt();
                for (int i = 0; i < numTitle; i++)
                    tmp = _in.readInt();

                // skip body
                numBody = _in.readInt();
                for (int i = 0; i < numBody; i++)
                    tmp = _in.readInt();

                // read tags
                numTags = _in.readInt();
                for (int i = 0; i < numTags; i++) {
                    tmp = _in.readInt();
                    if (tmp < _maxTag)
                        _out.append(tmp + ",");
                }
                _out.append("\n");
            }
            _in.close();
            _out.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public void writeX(int head, int tail) {
        int id, tmp;

        DataInputStream in;
        BufferedWriter out;

        try {
            for (int i = 0; i < 603; i++) {
                in = new DataInputStream(new BufferedInputStream(
                        new FileInputStream(OUTPUT_DIR + "out" + i + ".txt"),
                        BUFFER_SIZE));
                out = new BufferedWriter(new FileWriter(OUTPUT_DIR + "X" + i
                        + "train.csv"), BUFFER_SIZE);

                writeWordsInX(in, out, head, tail);
            }
            // in = new DataInputStream(new BufferedInputStream(
            // new FileInputStream(OUTPUT_DIR + "out1.txt"), BUFFER_SIZE));
            // out = new BufferedWriter(new FileWriter(OUTPUT_DIR + "Xcv.csv"),
            // BUFFER_SIZE);
            //
            // writeWordsInX(in, out, head, tail);
            //
            // in = new DataInputStream(new BufferedInputStream(
            // new FileInputStream(OUTPUT_DIR + "out2.txt"), BUFFER_SIZE));
            // out = new BufferedWriter(new FileWriter(OUTPUT_DIR +
            // "Xtest.csv"),
            // BUFFER_SIZE);
            //
            // writeWordsInX(in, out, head, tail);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    private void writeWordsInX(DataInputStream _in, BufferedWriter _out,
            int _head, int _tail) {
        int numTitle, numBody, numTags;
        int id, tmp;
        try {
            while (_in.available() > 0) {

                // skip id;
                id = _in.readInt();

                // add title
                numTitle = _in.readInt();
                for (int i = 0; i < numTitle; i++)
                    _out.append(_in.readInt() + ",");

                // add body
                numBody = _in.readInt();
                for (int i = 0; i < numBody; i++)
                    if (i + numTitle < _head || i + numTitle >= numBody - _tail)
                        _out.append(_in.readInt() + ",");
                    else
                        _in.readInt();

                // skip tags
                numTags = _in.readInt();
                for (int i = 0; i < numTags; i++)
                    tmp = _in.readInt();
                _out.append("\n");
            }
            _in.close();
            _out.close();

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    private void removeCentralStringBody(String dir, String prefix,
            int numberOfFiles, int head, int tail) {

        int id, titleWords, bodyWords, title, body, tags, tag;

        try {

            for (int i = 0; i < numberOfFiles; i++) {

                DataInputStream in = new DataInputStream(
                        new BufferedInputStream(new FileInputStream(dir
                                + prefix + i + ".txt")));
                DataOutputStream out = new DataOutputStream(
                        new BufferedOutputStream(new FileOutputStream(dir
                                + prefix + "SmallBody" + i + ".txt")));
                BufferedWriter outCsv = new BufferedWriter(new FileWriter(dir
                        + prefix + "SmallBody" + i + ".csv"));

                while (in.available() > 0) {
                    // read id
                    id = in.readInt();
                    out.writeInt(id);
                    outCsv.append(String.valueOf(id));

                    // read title
                    title = in.readInt();
                    out.writeInt(title);
                    for (int t = 0; t < title; t++) {
                        titleWords = in.readInt();
                        out.writeInt(titleWords);
                        outCsv.append("," + titleWords);
                    }
                    // append zeros if the title size is less than
                    // MAX_TITLE_SIZE
                    for (int t = 0; t < MAX_TITLE_SIZE - title; t++) {
                        outCsv.append(",0");
                    }

                    // read body
                    body = in.readInt();
                    out.writeInt(head + tail);
                    if (head + tail > body) {
                        for (int t = 0; t < body / 2; t++) {
                            bodyWords = in.readInt();
                            out.writeInt(bodyWords);
                            outCsv.append("," + bodyWords);
                        }
                        for (int t = 0; t < head + tail - body; t++) {
                            out.writeInt(0);
                            outCsv.append(",0");
                        }
                        for (int t = body / 2; t < body; t++) {
                            bodyWords = in.readInt();
                            out.writeInt(bodyWords);
                            outCsv.append("," + bodyWords);
                        }
                    } else
                        for (int t = 0; t < body; t++) {
                            bodyWords = in.readInt();
                            if (t < head || t >= body - tail) {
                                out.writeInt(bodyWords);
                                outCsv.append("," + bodyWords);
                            }
                        }

                    // read tags
                    tags = in.readInt();
                    out.writeInt(tags);
                    for (int t = 0; t < tags; t++) {
                        tag = in.readInt();
                        out.writeInt(tag);
                        outCsv.append("," + tag);
                    }

                    outCsv.append("\n");
                }

                in.close();
                out.close();
                outCsv.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public void removeCommonWords(String dir, String prefix, int numberOfFiles,
            double maxSharedTags) {

        HashMap<Integer, HashSet<Integer>> wordMap = new HashMap<Integer, HashSet<Integer>>();
        int id, titleWords, bodyWords, title, body, tags, numTags, tag;

        numTags = createWordMap(wordMap, dir, prefix, numberOfFiles);

        try {

            for (int i = 0; i < numberOfFiles; i++) {

                DataInputStream in = new DataInputStream(
                        new BufferedInputStream(new FileInputStream(dir
                                + prefix + i + ".txt")));

                DataOutputStream out = new DataOutputStream(
                        new BufferedOutputStream(new FileOutputStream(dir
                                + prefix + "UncommonWords" + i + ".txt")));
                BufferedWriter outCsv = new BufferedWriter(new FileWriter(dir
                        + prefix + "UncommonWords" + i + ".csv"));

                while (in.available() > 0) {
                    // read id
                    id = in.readInt();
                    out.writeInt(id);
                    outCsv.append(id + ",");

                    // read title
                    title = in.readInt();

                    // read title words and create a list of uncommon words
                    int count = 0;
                    LinkedList<Integer> temp = new LinkedList<Integer>();
                    for (int t = 0; t < title; t++) {
                        titleWords = in.readInt();
                        if (wordMap.get(titleWords).size() < maxSharedTags
                                * numTags) {
                            temp.add(titleWords);
                            outCsv.append(titleWords + ",");
                        } else
                            count++;
                    }

                    // write the list of uncommon words
                    out.writeInt(title - count);
                    for (int t : temp)
                        out.writeInt(t);

                    // read body
                    body = in.readInt();

                    // read body words and create a list of uncommon words
                    count = 0;
                    temp = new LinkedList<Integer>();
                    for (int t = 0; t < body; t++) {
                        bodyWords = in.readInt();
                        if (wordMap.get(bodyWords).size() < maxSharedTags
                                * numTags) {
                            temp.add(bodyWords);
                            outCsv.append(bodyWords + ",");
                        } else
                            count++;
                    }

                    // write the list of uncommon words
                    out.writeInt(body - count);
                    for (int t : temp)
                        out.writeInt(t);

                    // read tags
                    tags = in.readInt();
                    for (int t = 0; t < tags; t++) {
                        tag = in.readInt();
                        out.writeInt(tag);
                        outCsv.append("," + tag);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

    }

    private int createWordMap(HashMap<Integer, HashSet<Integer>> map,
            String dir, String prefix, int numberOfFiles) {
        int id, titleWords, bodyWords, title, body, tags;
        HashSet<Integer> tagList = new HashSet<Integer>(70000);
        HashSet<Integer> wordList = new HashSet<Integer>(7000000);
        HashSet<Integer> tag = new HashSet<Integer>(10);
        
        HashSet<Integer> tmp;

        try {

            for (int i = 0; i < numberOfFiles; i++) {

                int count = 0;
                long start;
                
                wordList.clear();

                DataInputStream in = new DataInputStream(
                        new BufferedInputStream(new FileInputStream(dir
                                + prefix + i + ".txt")));

                while (in.available() > 0) {
                    if(count++ % 1 == 0)
                        System.out.println(count/1);
                    
                    tag.clear();
                    // read id
                    id = in.readInt();

                    // read title
                    start = System.currentTimeMillis();
                    title = in.readInt();
                    for (int t = 0; t < title; t++)
                        wordList.add(in.readInt());
                    System.out.println("Title read in: " + (System.currentTimeMillis() - start));
                    
                    // read body
                    start = System.currentTimeMillis();
                    body = in.readInt();
                    for (int t = 0; t < body; t++)
                        wordList.add(in.readInt());
                    System.out.println("Body read in: " + (System.currentTimeMillis() - start));

                    // read tags
                    start = System.currentTimeMillis();
                    tags = in.readInt();
                    for (int t = 0; t < tags; t++) {
                        int temp = in.readInt();
                        // add the tag to the list of this post
                        tag.add(temp);
                        
                        // add the tag to the whole lists of tags
                        tagList.add(temp);
                    }
                    System.out.println("Tags read in: " + (System.currentTimeMillis() - start));

                    // add tags to map
                    start = System.currentTimeMillis();
                    for (Integer w : wordList) {
                        if (w > 0) {
                            if (!map.containsKey(w))
                                map.put(w, tag);
                            else {
                                tmp = map.get(w);
                                tmp.addAll(tag);
                                map.put(w, tmp);
                            }
                        }
                    }
                    System.out.println("Map updated in: " + (System.currentTimeMillis() - start));

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        return tagList.size();
    }

    public static void main(String[] args) {
        Facebook f = new Facebook(10000);
        // f.createSymbolTable("./Train.csv");
        // f.writeOutputFiles("./Train.csv");
        // f.removeCommonWords();
        // f.createTagRecurrenceMap();
        // f.train("/home/storassa/Kaggle/Facebook/Train.csv");
        // f.writeCsvTagFile();
        // f.writeY(Integer.MAX_VALUE);
        // f.writeX(30, 30);
        // f.removeUncommonTags(0.001, 15);
        // f.removeCentralStringBody(OUTPUT_DIR + "/removedTags/",
        //        "outCommonTags", 15, 50, 50);
        f.removeCommonWords(OUTPUT_DIR + "removedTags/",
                "outCommonTagsSmallBody", 15, 0.1);
    }

    private ArrayList<String> getString(String s) {
        ArrayList<String> result = new ArrayList<String>();

        Pattern p = Pattern
                .compile("([a-zA-Z]+(\\.[a-zA-Z]+)?(\\([a-zA-Z]*\\))?)|(</?[a-zA-Z]+>)");
        Matcher m = p.matcher(s);
        while (m.find()) {
            String temp = m.group();
            if (temp.length() > 2) {
                result.add(temp);
                wordsTotal++;
                incWordsTotal++;
            }
        }

        return result;
    }

    private boolean process(String s, State state) {

        String fields[];
        switch (state) {
        case ID:
            try {
                fields = s.split(",");
                return process(s.substring(fields[0].length() + 2), State.TITLE);
            } catch (Exception e) {
                return process("", State.TITLE);
            }

        case TITLE:
            try {
                fields = s.split("\"");
                title = fields[0].substring(0, fields[0].length());
                return process(s.substring(fields[0].length() + 3), State.BODY);
            } catch (Exception e) {
                return process("", State.BODY);
            }

        case BODY:
            try {
                fields = s.split("\"");
                body += fields[0].substring(0, fields[0].length());
                if (fields.length == 1)
                    return false;
                else {
                    return process(
                            s.substring(fields[0].length() + 3, s.length() - 1),
                            State.TAGS);
                }
            } catch (Exception e) {
                return process("", State.TAGS);
            }

        case TAGS:
            tags = new ArrayList<String>(Arrays.asList(s.split(" ")));
            return true;
        }

        return true;
    }

    private static final double PERC_THR_TAGS = 0.001;
    private static final int POST_PER_FILE = 10000;
    private static final String OUTPUT_DIR = "./files/";
    private static final int BUFFER_SIZE = 32 * 1024;
    private static final int MAX_TITLE_SIZE = 30;
}
