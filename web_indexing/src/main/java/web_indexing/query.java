package web_indexing;

import java.io.IOException;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.zip.GZIPInputStream;

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
            int docIdsCount = 0;
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
            // System.out.println("docIds ====" + docIds);
            // System.out.println("frequencies ====" + frequencies);
        } catch (IOException e) {
            System.out.println("Unable to read content from file");
        }
    }
}

class SearchResult implements Comparable<SearchResult> {
    private String url;
    private Double score;
    private String snippet;

    SearchResult(String url, Double score, String snippet) {
        this.url = url;
        this.score = score;
        this.snippet = snippet;
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

    URLMapping(String url, Integer totalTermsCount) {
        this.url = url;
        this.totalTermsCount = totalTermsCount;
    }

    public String getUrl() {
        return url;
    }

    public Integer getTotalTermsCount() {
        return totalTermsCount;
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
            while ((currentTerm = br.readLine()) != null) {
                String[] docIdsToUrlMappingValues = currentTerm.split(" ");
                // System.out.println("currentTerm ======" + currentTerm);
                if (docIdsToUrlMappingValues.length == 3) {
                    Integer docId = Integer.parseInt(docIdsToUrlMappingValues[0]);
                    String url = docIdsToUrlMappingValues[1];
                    Integer totalTermsCount = Integer.parseInt(docIdsToUrlMappingValues[2]);
                    totalDocumentsTerms += totalTermsCount;
                    docIdToUrlMap.put(docId, new URLMapping(url, totalTermsCount));
                }
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

    public void findConjunctiveResults(List<PostingList> postingLists, PriorityQueue<SearchResult> result) {
        HashMap<Integer, Integer[]> distinctDocIdsFreqMap = new HashMap();
        ArrayList<Integer> ft = new ArrayList();
        for (int i = 0; i < postingLists.size(); i++) {
            PostingList pl = postingLists.get(i);
            ft.add(pl.getDocIdsSize());
            Integer d = 0;
            while ((d = pl.nextGEQ(d)) != null) {
                if (!distinctDocIdsFreqMap.containsKey(d)) {
                    distinctDocIdsFreqMap.put(d, new Integer[postingLists.size()]);
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
            URLMapping um = docIdToUrlMap.getOrDefault(did, new URLMapping(did + "", 0));
            SearchResult sr = new SearchResult(um.getUrl(), calculateBM25(ft, fdt, um.getTotalTermsCount()), "snippet");
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
        Integer did = 0, d = -1;
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
                URLMapping um = docIdToUrlMap.getOrDefault(did, new URLMapping(did + "", 0));
                SearchResult sr = new SearchResult(um.getUrl(), calculateBM25(ft, fdt, um.getTotalTermsCount()),
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

    public PriorityQueue<SearchResult> getSearchResults(String keyword, String queryType) {
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
        return result;
    }

    public static void main(String[] args) {
        Query query = new Query(10, "./invertedIndex");
        query.buildLexicon("./lexicon.gz");
        query.buildDocIdsToUrlMapping("./url_doc_mapping.gz");
        PriorityQueue<SearchResult> pq = query.getSearchResults("   america nigerian ", "conjunctive");
        while (pq.size() > 0) {
            SearchResult sr = pq.poll();
            System.out.println(
                    "url ===" + sr.getUrl() + " score======" + sr.getScore() + " snippet=======" + sr.getSnippet());
        }
        // PostingList pl = new PostingList();
        // pl.createPostings("./invertedIndex", 93553216, 5120);

    }
}