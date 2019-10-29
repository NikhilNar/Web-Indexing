package web_indexing;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

class InvertedIndex {
    private GZIPOutputStream lexiconFile;
    private GZIPOutputStream invertedIndexFile;
    private GZIPInputStream sortedTermsFile;

    InvertedIndex(String sortedTermsFilePath, String lexiconFilePath, String invertedIndexPath) {
        this.lexiconFile = createGzipFile(lexiconFilePath);
        this.invertedIndexFile = createGzipFile(invertedIndexPath);
        this.sortedTermsFile = openTermsFile(sortedTermsFilePath);
    }

    private GZIPInputStream openTermsFile(String fileName) {
        try {
            return new GZIPInputStream(new FileInputStream(fileName));
        } catch (IOException e) {
            System.out.println("Unable to create " + fileName);
        }
        return null;
    }

    private GZIPOutputStream createGzipFile(String fileName) {
        try {
            return new GZIPOutputStream(new FileOutputStream(fileName));
        } catch (IOException e) {
            System.out.println("Unable to create " + fileName);
        }
        return null;
    }

    public Boolean ifLexiconAndInvertedIndexDocumentCreated() {
        return lexiconFile != null && invertedIndexFile != null && sortedTermsFile != null;
    }

    public String findVarByte(Integer number) {
        String binaryValue = Integer.toBinaryString(number);
        StringBuffer sb = new StringBuffer();
        int counter = 7;
        Boolean lastBit = false;
        for (int i = binaryValue.length() - 1; i >= 0; i--) {
            Character bit = binaryValue.charAt(i);
            if (counter == 0) {
                String lastBitValue = lastBit ? "0" : "1";
                lastBit = true;
                sb.append(lastBitValue + bit);
                counter = 6;
            } else {
                sb.append(bit);
                counter--;
            }
        }
        while (counter > 0) {
            sb.append("0");
            counter--;
        }
        if (number <= 127) {
            sb.append("1");
        } else {
            sb.append("0");
        }
        return sb.reverse().toString();
    }

    public void createIndex() {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(sortedTermsFile));
            String previousTerm = null, currentTerm = null;
            TreeMap<Integer, Integer> docIdsToFreqMapping = new TreeMap();
            Integer totalBytes = 0;
            while ((currentTerm = br.readLine()) != null) {
                String[] posting = currentTerm.split(" ");
                if (posting[0].equals(previousTerm) || previousTerm == null) {
                    docIdsToFreqMapping.put(Integer.parseInt(posting[1]), Integer.parseInt(posting[2]));
                } else {
                    StringBuffer index = new StringBuffer();
                    StringBuffer docIdsVarByte = new StringBuffer();
                    StringBuffer frequenciesVarByte = new StringBuffer();
                    Integer lastDocId = null;
                    for (Map.Entry<Integer, Integer> entry : docIdsToFreqMapping.entrySet()) {
                        Integer currentDocId = entry.getKey();
                        Integer nextDocId = (lastDocId != null) ? currentDocId - lastDocId : currentDocId;
                        docIdsVarByte.append(findVarByte(nextDocId));
                        frequenciesVarByte.append(findVarByte(entry.getValue()));
                        lastDocId = currentDocId;
                    }
                    docIdsVarByte.append(frequenciesVarByte);
                    System.out.println("term =======" + posting[0]);
                    byte[] bytesForTerm = docIdsVarByte.toString().getBytes();
                    Integer totalBytesForTerm = bytesForTerm.length;
                    invertedIndexFile.write(bytesForTerm);
                    lexiconFile.write((previousTerm + " " + (totalBytes + 1) + " " + totalBytesForTerm + " "
                            + docIdsToFreqMapping.size() + " \n").getBytes());
                    docIdsToFreqMapping.clear();
                    docIdsToFreqMapping.put(Integer.parseInt(posting[1]), Integer.parseInt(posting[2]));
                    totalBytes += totalBytesForTerm;
                }
                previousTerm = posting[0];
            }
            sortedTermsFile.close();
            invertedIndexFile.finish();
            invertedIndexFile.close();
            lexiconFile.finish();
            lexiconFile.close();
        } catch (IOException e) {
            System.out.println("Error while reading the input" + e);
        }
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        InvertedIndex index = new InvertedIndex("./sorted.gz", "./lexicon.gz", "./invertedIndex.gz");
        if (index.ifLexiconAndInvertedIndexDocumentCreated())
            index.createIndex();
        System.out.println("Total time =" + (System.currentTimeMillis() - startTime) / 60000.0);
    }

}