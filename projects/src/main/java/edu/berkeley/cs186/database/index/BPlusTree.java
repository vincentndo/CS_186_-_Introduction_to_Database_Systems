package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.io.PageAllocator;
import edu.berkeley.cs186.database.table.RecordID;
import edu.berkeley.cs186.database.databox.*;

import java.util.*;
import java.nio.file.Paths;

/**
 * A B+ tree. Allows the user to add, delete, search, and scan for keys in an
 * index. A BPlusTree has an associated page allocator. The first page in the
 * page allocator is a header page that serializes the search key data type,
 * root node page, and first leaf node page. Each subsequent page is a
 * BPlusNode, specifically either an InnerNode or LeafNode. Note that a
 * BPlusTree can have duplicate keys that appear across multiple pages.
 *
 * Properties:
 * allocator: PageAllocator for this index
 * keySchema: DataBox for this index's search key
 * rootPageNum: page number of the root node
 * firstLeafPageNum: page number of the first leaf node
 * numNodes: number of BPlusNodes
 */
public class BPlusTree {
    public static final String FILENAME_PREFIX = "db";
    public static final String FILENAME_EXTENSION = ".index";

    protected PageAllocator allocator;
    protected DataBox keySchema;
    private int rootPageNum;
    private int firstLeafPageNum;
    private int numNodes;

    /**
     * This constructor is used for creating an empty BPlusTree.
     *
     * @param keySchema the schema of the index key
     * @param fName the filename of where the index will be built
     */
    public BPlusTree(DataBox keySchema, String fName) {
        this(keySchema, fName, FILENAME_PREFIX);
    }

    public BPlusTree(DataBox keySchema, String fName, String filePrefix) {
        String pathname = Paths.get(filePrefix, fName + FILENAME_EXTENSION).toString();
        this.allocator = new PageAllocator(pathname, true);
        this.keySchema = keySchema;
        int headerPageNum = this.allocator.allocPage();
        assert(headerPageNum == 0);
        this.numNodes = 0;
        BPlusNode root = new LeafNode(this);
        this.rootPageNum = root.getPageNum();
        this.firstLeafPageNum = rootPageNum;
        writeHeader();
    }

    /**
     * This constructor is used for loading a BPlusTree from a file.
     *
     * @param fName the filename of a preexisting BPlusTree
     */
    public BPlusTree(String fName) {
        this(fName, FILENAME_PREFIX);
    }

    public BPlusTree(String fName, String filePrefix) {
        String pathname = Paths.get(filePrefix, fName + FILENAME_EXTENSION).toString();
        this.allocator = new PageAllocator(pathname, false);
        this.readHeader();
    }

    public void incrementNumNodes() {
        this.numNodes++;
    }

    public void decrementNumNodes() {
        this.numNodes--;
    }

    public int getNumNodes() {
        return this.numNodes;
    }

    /**
     * Perform a sorted scan.
     * The iterator should return all RecordIDs, starting from the beginning to
     * the end of the index.
     *
     * @return Iterator of all RecordIDs in sorted order
     */
    public Iterator<RecordID> sortedScan() {
        BPlusNode rootNode = BPlusNode.getBPlusNode(this, rootPageNum);
        return new BPlusIterator(rootNode);
    }

    /**
     * Perform a range search beginning from a specified key.
     * The iterator should return all RecordIDs, starting from the specified
     * key to the end of the index.
     *
     * @param keyStart the key to start iterating from
     * @return Iterator of RecordIDs that are equal to or greater than keyStart
     * in sorted order
     */
    public Iterator<RecordID> sortedScanFrom(DataBox keyStart) {
        BPlusNode root = BPlusNode.getBPlusNode(this, rootPageNum);
        return new BPlusIterator(root, keyStart, true);
    }

    /**
     * Perform an equality search on the specified key.
     * The iterator should return all RecordIDs that match the specified key.
     *
     * @param key the key to match
     * @return Iterator of RecordIDs that match the given key
     */
    public Iterator<RecordID> lookupKey(DataBox key) {
        BPlusNode root = BPlusNode.getBPlusNode(this, rootPageNum);
        return new BPlusIterator(root, key, false);
    }

    /**
     * Insert a (Key, RecordID) tuple.
     *
     * @param key the key to insert
     * @param rid the RecordID of the given key
     */
    public void insertKey(DataBox key, RecordID rid) {
        // Implement me!
        BPlusNode root = BPlusNode.getBPlusNode(this, rootPageNum);
        LeafEntry newbie = new LeafEntry(key, rid);
        InnerEntry newRootEntry = root.insertBEntry(newbie);

        if (newRootEntry != null) {

                InnerNode newRoot= new InnerNode(this);
                newRoot.setFirstChild(root.getPageNum());
                List<BEntry> newRootEntries = new ArrayList<BEntry>(1);
                newRootEntries.add(newRootEntry);
                newRoot.overwriteBNodeEntries(newRootEntries);
                updateRoot(newRoot.getPageNum());

        }
    }

    /**
     * Delete an entry with the matching key and RecordID.
     *
     * @param key the key to be deleted
     * @param rid the RecordID of the key to be deleted
     */
    public boolean deleteKey(DataBox key, RecordID rid) {
        /* You will not have to implement this in this project. */
        throw new BPlusTreeException("BPlusTree#DeleteKey Not Implemented!");
    }

    /**
     * Perform an equality search on the specified key.
     *
     * @param key the key to lookup
     * @return true if the key exists in this BPlusTree, false otherwise
     */
    public boolean containsKey(DataBox key) {
        return lookupKey(key).hasNext();
    }

    /**
     * Return the number of pages.
     *
     * @return the number of pages.
     */
    public int getNumPages() {
        return this.allocator.getNumPages();
    }

    /**
     * Update the root page.
     *
     * @param pNum the page number of the new root node
     */
    protected void updateRoot(int pNum) {
        this.rootPageNum = pNum;
        writeHeader();
    }

    private void writeHeader() {
        Page headerPage = allocator.fetchPage(0);
        int bytesWritten = 0;

        headerPage.writeInt(bytesWritten, this.rootPageNum);
        bytesWritten += 4;

        headerPage.writeInt(bytesWritten, this.firstLeafPageNum);
        bytesWritten += 4;

        headerPage.writeInt(bytesWritten, keySchema.type().ordinal());
        bytesWritten += 4;

        if (this.keySchema.type().equals(DataBox.Types.STRING)) {
            headerPage.writeInt(bytesWritten, this.keySchema.getSize());
            bytesWritten += 4;
        }
        headerPage.flush();
    }

    private void readHeader() {
        Page headerPage = allocator.fetchPage(0);

        int bytesRead = 0;

        this.rootPageNum = headerPage.readInt(bytesRead);
        bytesRead += 4;

        this.firstLeafPageNum = headerPage.readInt(bytesRead);
        bytesRead += 4;

        int keyOrd = headerPage.readInt(bytesRead);
        bytesRead += 4;
        DataBox.Types type = DataBox.Types.values()[keyOrd];

        switch(type) {
            case INT:
                this.keySchema = new IntDataBox();
                break;
            case STRING:
                int len = headerPage.readInt(bytesRead);
                this.keySchema = new StringDataBox(len);
                break;
            case BOOL:
                this.keySchema = new BoolDataBox();
                break;
            case FLOAT:
                this.keySchema = new FloatDataBox();
                break;
        }
    }

    /**
     * A BPlusIterator provides several ways of iterating over RecordIDs stored
     * in a BPlusTree.
     */
    private class BPlusIterator implements Iterator<RecordID> {
        // Implement me!
        private class Pair {
            private BPlusNode node;
            private int pos;

            public Pair(BPlusNode node, int pos) {
                this.node = node;
                this.pos = pos;
            }

            public BPlusNode getNode () {
                return node;
            }

            public void setNode(BPlusNode node) {
                this.node = node;
            }

            public int getPos() {
                return pos;
            }

            public void setPos(int pos) {
                this.pos = pos;
            }
        }

        private Stack<Pair> nodeStack = new Stack<Pair>();
        private DataBox key = null;
        private boolean scan = true;
        public boolean hasNext;

        /**
         * Construct an iterator that performs a sorted scan on this BPlusTree
         * tree.
         * The iterator should return all RecordIDs, starting from the
         * beginning to the end of the index.
         *
         * @param root the root node of this BPlusTree
         */
        public BPlusIterator(BPlusNode root) {
            // Implement me!
            Pair rootPair = new Pair(root, 0);
            this.nodeStack.push(rootPair);

            while (!root.isLeaf()) {

                InnerNode rootNode = (InnerNode) root;
                BPlusNode childNode = BPlusNode.getBPlusNode(root.getTree(), rootNode.getFirstChild());

                Pair childNodePair = new Pair(childNode, 0);

                this.nodeStack.push(childNodePair);
                root = childNode;
            }
            this.hasNext = hasNext();
        }

        /**
         * Construct an iterator that performs either an equality or range
         * search with a specified key.
         * If @param scan is true, the iterator should return all RecordIDs,
         * starting from the specified key to the end of the index.
         * If @param scan is false, the iterator should return all RecordIDs
         * that match the specified key.
         *
         * @param root the root node of this BPlusTree
         * @param key the specified key value
         * @param scan if true, do a range search; else, equality search
         */
        public BPlusIterator(BPlusNode root, DataBox key, boolean scan) {
            // Implement me!
            this.key = key;
            this.scan = scan;

            int p;
            List<BEntry> allEntries = root.getAllValidEntries();
            int size = allEntries.size();

            for (p = 0; p < size; p++) {
                if (allEntries.get(p).getKey().compareTo(key) >= 0) {
                    break;
                }
            }
            Pair rootPair = new Pair(root, p);
            this.nodeStack.push(rootPair);

            while (!root.isLeaf()) {

                InnerNode rootNode = (InnerNode) root;
                BPlusNode childNode;
                if (rootPair.getPos() == 0) {
                    childNode = BPlusNode.getBPlusNode(root.getTree(), rootNode.getFirstChild());
                } else {
                    int pageNum = root.getAllValidEntries().get(rootPair.getPos() - 1).getPageNum();
                    childNode = BPlusNode.getBPlusNode(root.getTree(), pageNum);
                }

                allEntries = childNode.getAllValidEntries();
                size = allEntries.size();
                root = childNode;

                if (!childNode.isLeaf()) {
                    for (p = 0; p <= size; p++) {
                        if (allEntries.get(p).getKey().compareTo(key) >= 0) {
                            break;
                        }
                    }
                    rootPair = new Pair(childNode, p);
                } else {
                    for (p = 0; p < size; p++) {
                        if (allEntries.get(p).getKey().compareTo(key) >= 0) {
                            break;
                        }
                    }
                    rootPair = new Pair(childNode, Math.min(p, size - 1));
                }

                this.nodeStack.push(rootPair);

            }
            this.hasNext = hasNext();
        }

        /**
         * Confirm if iterator has more RecordIDs to return.
         *
         * @return true if there are still RecordIDs to be returned, false
         * otherwise
         */
        public boolean hasNext() {
            // Implement me!

            if (nodeStack.isEmpty()) {
                this.hasNext = false;
                return false;
            }

            BPlusNode node;
            int pos;
            List<BEntry> allEntries;

            if (!scan) {
                Pair nodePair = nodeStack.peek();
                node = nodePair.getNode();
                pos = nodePair.getPos();
                allEntries = node.getAllValidEntries();

                if (pos == allEntries.size()) {
                    this.hasNext = false;
                    return false;
                } else {
                    DataBox key = allEntries.get(pos).getKey();

                    if (this.key.compareTo(key) < 0) {
                        this.hasNext = false;
                        return false;
                    }
                }
            }

            for (Pair nodePair : nodeStack) {

                node = nodePair.getNode();
                pos = nodePair.getPos();
                allEntries = node.getAllValidEntries();

                if (pos < allEntries.size()) {
                    this.hasNext = true;
                    return true;
                }
            }
            this.hasNext = false;
            return false;
        }

        private void advance () {

            if (nodeStack.isEmpty()) {
                return;
            }

            Pair nodePair = nodeStack.pop();
            BPlusNode node = nodePair.getNode();
            int pos = nodePair.getPos();
            List<BEntry> allEntries = node.getAllValidEntries();

            if (node.isLeaf() && pos < allEntries.size() - 1) {

                pos += 1;
                nodePair.setPos(pos);
                this.nodeStack.push(nodePair);

//                DataBox key = allEntries.get(pos).getKey();
//                if (!scan) {
//                    if (this.key.compareTo(key) != 0) {
//                        advance();
//                    }
//                }
            } else if (node.isLeaf() && pos == allEntries.size() - 1) {
                advance();
            } else if (!node.isLeaf() && pos < allEntries.size()) {
                pos += 1;
                nodePair.setPos(pos);
                this.nodeStack.push(nodePair);

                while (!node.isLeaf()) {
                    InnerNode innerNode = (InnerNode) node;
                    List<BEntry> allInnerEntries = innerNode.getAllValidEntries();
                    int pageNum = allInnerEntries.get(pos - 1).getPageNum();
                    BPlusNode childNode = BPlusNode.getBPlusNode(node.getTree(), pageNum);

                    Pair childNodePair = new Pair(childNode, 0);

                    this.nodeStack.push(childNodePair);
                    node = childNode;
                }

//                allEntries = node.getAllValidEntries();
//                DataBox key = allEntries.get(pos).getKey();
//                if (!scan) {
//                    if (this.key.compareTo(key) != 0) {
//                        advance();
//                    }
//                }
            } else {
                advance();
            }
        }

        /**
         * Yield the next RecordID of this iterator.
         *
         * @return the next RecordID
         * @throws NoSuchElementException if there are no more RecordIDs to
         * yield
         */
        public RecordID next() {
            // Implement me!
            if (this.hasNext) {

                Pair leafNodePair = nodeStack.peek();
                LeafNode leafNode = (LeafNode) leafNodePair.getNode();
                int pos = leafNodePair.getPos();
                List<BEntry> allEntries = leafNode.getAllValidEntries();
                DataBox key = allEntries.get(pos).getKey();
                RecordID rid = allEntries.get(pos).getRecordID();
                advance();

                if (this.key != null) {
                    if (this.key.compareTo(key) > 0) {
                        return this.next();
                    } else if (this.key.compareTo(key) == 0){
                        return rid;
                    } else {
                        if (scan) {
                            return rid;
                        } else {
                            return this.next();
                        }
                    }
                } else {
                    return rid;
                }

            } else {
                throw new NoSuchElementException("The End of the World!");
            }
        }

        public void remove() {
            /* You will not have to implement this in this project. */
            throw new UnsupportedOperationException();
        }
    }
}
