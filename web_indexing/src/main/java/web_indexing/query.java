package web_indexing;

import java.io.IOException;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.zip.GZIPInputStream;
import java.util.LinkedList;

class Posting {
    private Integer offset;
    private Integer size;
    private Integer count;

    Posting(Integer offset, Integer size, Integer count) {
        this.offset = offset;
        this.size = size;
        this.count = count;
    }

    Integer getOffset() {
        return offset;
    }

    Integer getSize() {
        return size;
    }

    Integer getCount() {
        return count;
    }

}

class PostingList {
    private List<Integer> docIds;
    private List<Integer> frequencies;
    private Integer index;

    PostingList() {
        docIds = new ArrayList();
        frequencies = new ArrayList();
        index = -1;
    }

    public String[] decodeVarByteCompression(String varByteEncoding) {
        String[] decodings = new String[varByteEncoding.length() / 8];
        int j = 0;
        for (int i = 0; i < decodings.length; i++) {
            decodings[i] = varByteEncoding.substring(j, j + 8);
            j += 8;
        }
        return decodings;
    }

    public Integer nextGEQ(Integer k) {
        if (index > -1 && k != null) {
            for (int i = index; i < docIds.size(); i++) {
                if (docIds.get(i) >= k)
                    return docIds.get(i);
            }
        }
        return null;
    }

    public Integer getDocIdsSize() {
        return docIds.size();
    }

    public Integer getFreq() {
        return (index > -1) ? frequencies.get(index) : null;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    public void createPostings(String fileName, Integer offset, Integer size) {
        try {
            RandomAccessFile invertedIndexFile = new RandomAccessFile(fileName, "r");
            invertedIndexFile.seek(offset);
            byte[] byteArray = new byte[size];
            invertedIndexFile.read(byteArray);
            String varByteEncoding = new String(byteArray);
            // System.out.println("Byte accessed =" + varByteEncoding);
            String[] bytes = decodeVarByteCompression(varByteEncoding);
            StringBuffer sb = new StringBuffer();
            ArrayList<Integer> varByteDecodingList = new ArrayList();
            for (String byteString : bytes) {
                sb.append(byteString.substring(1));
                if (byteString.charAt(0) == '1') {
                    varByteDecodingList.add(Integer.parseInt(sb.toString(), 2));
                    sb = new StringBuffer();
                }
            }
            int i = 0;
            Integer docIdsCount = 0;
            for (Integer no : varByteDecodingList) {
                i++;
                if (i <= varByteDecodingList.size() / 2) {
                    docIdsCount += no;
                    docIds.add(docIdsCount);
                } else {
                    frequencies.add(no);
                }
            }
            if (docIds.size() > 0) {
                index++;
            }
            invertedIndexFile.close();
            // System.out.println("docIds ====" + docIds);
            // System.out.println("frequencies ====" + frequencies);
        } catch (IOException e) {
            System.out.println("Unable to read content from file");
        }
    }
}

class SearchResult implements Comparable<SearchResult> {
    private String url;
    private Integer documentId;
    private Double score;
    private String snippet;
    private HashMap<String, Integer> wordsFrequenciesMap;

    SearchResult(String url, Integer documentId, Double score, String snippet) {
        this.url = url;
        this.documentId = documentId;
        this.score = score;
        this.snippet = snippet;
        wordsFrequenciesMap = new HashMap();
    }

    public String getUrl() {
        return url;
    }

    public Double getScore() {
        return score;
    }

    public String getSnippet() {
        return snippet;
    }

    public Integer getDocumentId() {
        return documentId;
    }

    public HashMap<String, Integer> getWordsFrequenciesMap() {
        return wordsFrequenciesMap;
    }

    public void setSnippet(String snippet) {
        this.snippet = snippet;
    }

    @Override
    public int compareTo(SearchResult sr) {
        if (this.score > sr.score)
            return 1;
        else if (this.score < sr.score)
            return -1;
        else
            return 0;
    }
}

class URLMapping {
    private String url;
    private Integer totalTermsCount;
    private String documentFileName;
    private Integer offset;
    private Integer size;

    URLMapping(String url, Integer totalTermsCount, String documentFileName, Integer offset, Integer size) {
        this.url = url;
        this.totalTermsCount = totalTermsCount;
        this.documentFileName = documentFileName;
        this.offset = offset;
        this.size = size;
    }

    public String getUrl() {
        return url;
    }

    public Integer getTotalTermsCount() {
        return totalTermsCount;
    }

    public String getDocumentFileName() {
        return documentFileName;
    }

    public Integer getOffset() {
        return offset;
    }

    public Integer getSize() {
        return size;
    }
}

class Query {
    private HashMap<String, Posting> lexiconMap;
    private HashMap<Integer, URLMapping> docIdToUrlMap;
    private Integer totalResults;
    private String invertedIndexPath;
    private Integer totalDocumentsTerms;

    Query(Integer totalResults, String invertedIndexPath) {
        lexiconMap = new HashMap();
        docIdToUrlMap = new HashMap();
        this.totalResults = totalResults;
        this.invertedIndexPath = invertedIndexPath;
        this.totalDocumentsTerms = 0;
    }

    public void buildLexicon(String fileName) {
        try {
            GZIPInputStream lexiconFile = new GZIPInputStream(new FileInputStream(fileName));
            BufferedReader br = new BufferedReader(new InputStreamReader(lexiconFile));
            String currentTerm = null;
            while ((currentTerm = br.readLine()) != null) {
                String[] lexiconValues = currentTerm.split(" ");
                // System.out.println("currentTerm ======" + currentTerm);
                if (lexiconValues.length == 4) {
                    String term = lexiconValues[0];
                    Integer offset = Integer.parseInt(lexiconValues[1]) - 1;
                    Integer size = Integer.parseInt(lexiconValues[2]);
                    Integer count = Integer.parseInt(lexiconValues[3]);
                    lexiconMap.put(term, new Posting(offset, size, count));
                }
            }
            System.out.println("lexiconMapSize =====" + lexiconMap.size());
            lexiconFile.close();
        } catch (IOException e) {
            System.out.println("Unable to read content from file");
        }
        // lexiconMap.put("america", new Posting(11150544, 13888, 839));
        // lexiconMap.put("nigerian", new Posting(84141512, 1376, 63));
        // lexiconMap.put("nobel", new Posting(84351832, 1448, 67));
    }

    public void buildDocIdsToUrlMapping(String fileName) {
        try {
            GZIPInputStream lexiconFile = new GZIPInputStream(new FileInputStream(fileName));
            BufferedReader br = new BufferedReader(new InputStreamReader(lexiconFile));
            String currentTerm = null;
            HashMap<Integer, Integer> entries = new HashMap();
            entries.put(1000, 1);
            entries.put(10000, 1);
            entries.put(100000, 1);
            entries.put(500000, 1);
            entries.put(600000, 1);
            entries.put(700000, 1);
            entries.put(800000, 1);
            entries.put(900000, 1);
            entries.put(1000000, 1);
            entries.put(1200000, 1);
            entries.put(1400000, 1);
            entries.put(1600000, 1);
            entries.put(1800000, 1);
            entries.put(2000000, 1);
            entries.put(3000000, 1);
            entries.put(4000000, 1);
            entries.put(5000000, 1);
            entries.put(6000000, 1);
            Integer i = 0;
            while ((currentTerm = br.readLine()) != null) {
                String[] docIdsToUrlMappingValues = currentTerm.split(" ");
                // System.out.println("currentTerm ======" + currentTerm);
                if (docIdsToUrlMappingValues.length == 6) {
                    Integer docId = Integer.parseInt(docIdsToUrlMappingValues[0]);
                    String url = docIdsToUrlMappingValues[1];
                    Integer totalTermsCount = Integer.parseInt(docIdsToUrlMappingValues[2]);
                    String documentFileName = docIdsToUrlMappingValues[3];
                    try {
                        Integer offset = Integer.parseInt(docIdsToUrlMappingValues[4]) - 1;
                        Integer size = Integer.parseInt(docIdsToUrlMappingValues[5]);
                        totalDocumentsTerms += totalTermsCount;
                        docIdToUrlMap.put(docId, new URLMapping(url, totalTermsCount, documentFileName, offset, size));
                    } catch (Exception e) {
                        System.out.println("Exception caught" + e);
                    }

                }
                if (entries.containsKey(i)) {
                    System.out.println("i=" + i);
                }
                i++;
            }
            System.out.println("buildDocIdsToUrlMappingSize =====" + docIdToUrlMap.size());
            lexiconFile.close();
        } catch (IOException e) {
            System.out.println("Unable to read content from file");
        }

    }

    public Double calculateBM25(ArrayList<Integer> ft, ArrayList<Integer> fdt, Integer modd) {
        // Random r = new Random();
        // return 0 + 100 * r.nextDouble();
        Double score = 0.0;
        Integer N = docIdToUrlMap.size();
        Double moddavg = (double) totalDocumentsTerms / N;
        double k1 = 1.2, b = 0.75;
        for (int i = 0; i < ft.size(); i++) {
            Double logarithmicTerm = (N - ft.get(i) + 0.5) / (ft.get(i) + 0.5);
            Double K = k1 * ((1 - b) + b * modd / moddavg);
            Double secondTerm = (k1 + 1) * fdt.get(i) / (K + fdt.get(i));
            score += Math.log(logarithmicTerm) * secondTerm;
        }
        return score;
    }

    public int lastCapitalIndex(String content) {
        int index = 0;
        for (int i = content.length() - 1; i >= 0; i--) {
            char letter = content.charAt(i);
            if (Character.isUpperCase(letter) && (i + 1 <= content.length()
                    && (Character.isLowerCase(content.charAt(i + 1)) || content.charAt(i + 1) == ' '))) {
                return i;
            }
        }

        return index;
    }

    public String createSnippet(String content, String[] words) {
        String contentLowerCase = content.toLowerCase();
        ArrayList<Integer> indices = new ArrayList();
        LinkedList<Integer> queue = new LinkedList();
        for (String word : words) {
            if (word.length() == 0) {
                continue;
            }
            int index = contentLowerCase.indexOf(word);
            if (index >= 0) {
                indices.add(index);
            }
            while (index >= 0) {
                index = contentLowerCase.indexOf(word, index + word.length());
                if (index >= 0) {
                    indices.add(index);
                }
            }
        }
        Collections.sort(indices);
        int startIndex = indices.get(0), endIndex = startIndex, maxIndices = 1, noOfCharsDiff = 300, diffInQueue = 0;
        queue.add(startIndex);
        for (int i = 1; i < indices.size() - 1; i++) {
            Integer diff = indices.get(i) - indices.get(i - 1);
            while (diffInQueue + diff > noOfCharsDiff && queue.size() > 0) {
                int diffAfterRemoval = 0;
                int removedValue = queue.poll();
                if (queue.size() > 1) {
                    diffAfterRemoval = queue.get(0) - removedValue;
                }
                diffInQueue -= diffAfterRemoval;
            }
            queue.add(indices.get(i));
            if (queue.size() > maxIndices) {
                startIndex = queue.get(0);
                endIndex = queue.get(queue.size() - 1);
                maxIndices = queue.size();
            }
        }
        String start = "";
        String preIndex = content.substring(0, startIndex);
        int preIndexValue = -1;
        if ((preIndexValue = preIndex.lastIndexOf(".")) > 0) {
            start = content.substring(preIndexValue + 1, startIndex);
        } else {
            start = content.substring(lastCapitalIndex(content.substring(0, startIndex)), startIndex);
        }

        String snippetContent = start + content.substring(startIndex);

        return snippetContent.substring(0, Math.min(497, snippetContent.length())) + "...";
    }

    public void generateSnippet(SearchResult sr, String[] words) {
        URLMapping um = docIdToUrlMap.get(sr.getDocumentId());
        try {
            RandomAccessFile invertedIndexFile = new RandomAccessFile(um.getDocumentFileName(), "r");
            invertedIndexFile.seek(um.getOffset());
            byte[] byteArray = new byte[um.getSize()];
            invertedIndexFile.read(byteArray);
            String content = new String(byteArray);
            System.out.println("content ==========" + content);
            String snippet = createSnippet(content, words);
            sr.setSnippet(snippet);
            invertedIndexFile.close();
        } catch (IOException e) {
            System.out.println("Unable to read file");
        }

    }

    public void findConjunctiveResults(List<PostingList> postingLists, PriorityQueue<SearchResult> result) {
        HashMap<Integer, Integer[]> distinctDocIdsFreqMap = new HashMap();
        ArrayList<Integer> ft = new ArrayList();
        for (int i = 0; i < postingLists.size(); i++) {
            PostingList pl = postingLists.get(i);
            ft.add(pl.getDocIdsSize());
            Integer d = new Integer(0);
            while ((d = pl.nextGEQ(d)) != null) {
                if (!distinctDocIdsFreqMap.containsKey(d)) {
                    Integer[] intArray = new Integer[postingLists.size()];
                    for (int j = 0; j < intArray.length; j++) {
                        intArray[j] = 0;
                    }
                    distinctDocIdsFreqMap.put(d, intArray);
                }
                Integer[] postingFreqs = distinctDocIdsFreqMap.get(d);
                postingFreqs[i] = pl.getFreq();
                d++;
            }
        }

        for (Integer did : distinctDocIdsFreqMap.keySet()) {
            ArrayList<Integer> fdt = new ArrayList();
            for (Integer freq : distinctDocIdsFreqMap.get(did)) {
                fdt.add(freq);
            }
            URLMapping um = docIdToUrlMap.get(did);
            SearchResult sr = new SearchResult(um.getUrl(), did, calculateBM25(ft, fdt, um.getTotalTermsCount()),
                    "snippet");
            if (result.size() == totalResults) {
                if (result.peek().getScore() < sr.getScore()) {
                    result.poll();
                    result.add(sr);
                }
            } else {
                result.add(sr);
            }
        }
    }

    public void findDisjunctiveResults(List<PostingList> postingLists, PriorityQueue<SearchResult> result) {
        System.out.println("in findDisjunctiveResults========");
        Integer did = new Integer(0), d = new Integer(-1);
        ArrayList<Integer> ft = new ArrayList();
        for (PostingList currentPL : postingLists) {
            ft.add(currentPL.getDocIdsSize());
        }
        while (did != null && postingLists.size() > 0) {
            did = postingLists.get(0).nextGEQ(did);
            for (int i = 1; i < postingLists.size(); i++) {
                if (postingLists.get(i) == null) {
                    return;
                }
                d = postingLists.get(i).nextGEQ(did);
                if (d != null && d.equals(did)) {
                    continue;
                } else {
                    break;
                }
            }
            ;
            if (d == null || did == null)
                break;
            if (d > did)
                did = d;
            else {
                // get frequencies and calcuate BM25
                ArrayList<Integer> fdt = new ArrayList();
                for (int i = 0; i < postingLists.size(); i++) {
                    fdt.add(postingLists.get(i).getFreq());
                }
                URLMapping um = docIdToUrlMap.get(did);
                SearchResult sr = new SearchResult(um.getUrl(), did, calculateBM25(ft, fdt, um.getTotalTermsCount()),
                        "snippet");
                if (result.size() == totalResults) {
                    if (result.peek().getScore() < sr.getScore()) {
                        result.poll();
                        result.add(sr);
                    }
                } else {
                    result.add(sr);
                }
                did++;
            }
        }

    }

    public List<SearchResult> getSearchResults(String keyword, String queryType) {
        PriorityQueue<SearchResult> result = new PriorityQueue();
        String[] words = keyword.split(" ");
        List<PostingList> postingLists = new ArrayList();
        for (String word : words) {
            if (word.length() == 0) {
                continue;
            }
            PostingList pl = null;
            Posting p = lexiconMap.get(word);
            if (p != null) {
                Integer offset = p.getOffset();
                Integer size = p.getSize();
                pl = new PostingList();
                pl.createPostings(invertedIndexPath, offset, size);
            }
            postingLists.add(pl);
        }
        switch (queryType) {
        case "conjunctive":
            findConjunctiveResults(postingLists, result);
            break;
        case "disjunctive":
            findDisjunctiveResults(postingLists, result);
        }
        List<SearchResult> finalListOfUrls = new ArrayList();
        while (result.size() > 0) {
            SearchResult sr = result.poll();
            generateSnippet(sr, words);
            finalListOfUrls.add(sr);
        }

        return finalListOfUrls;
    }

    public static void main(String[] args) throws IOException {
        Query query = new Query(10, "./invertedIndex");
        query.buildLexicon("./lexicon.gz");
        query.buildDocIdsToUrlMapping("./url_doc_mapping.gz");

        SearchResult sr = new SearchResult("url", 69476, 2.0, "snippet");
        String[] words = new String[3];
        words[0] = "america";
        words[1] = "nigeria";
        words[2] = "nobel";
        query.generateSnippet(sr, words);

        // while (true) {
        // BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        // System.out.print("Enter query or enter exit to quit : ");
        // String input = br.readLine();
        // if (input.equals("exit")) {
        // break;
        // }
        // System.out.print("Enter type of query(conjunctive or disjunctive) : ");
        // String queryType = br.readLine();
        // long startTime = System.currentTimeMillis();
        // List<SearchResult> l = query.getSearchResults(input, queryType);
        // for (SearchResult sr : l) {
        // System.out.println("URL =" + sr.getUrl());
        // System.out.println("score =" + sr.getScore());
        // System.out.println("");
        // System.out.println(sr.getSnippet());
        // System.out.println("====================================================================");
        // }
        // System.out.println("Total time =" + (System.currentTimeMillis() - startTime)
        // / 1000.0 + " s");

        // }

    }
}