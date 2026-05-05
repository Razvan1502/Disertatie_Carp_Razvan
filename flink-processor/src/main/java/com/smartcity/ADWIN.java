package com.smartcity;

import java.io.Serializable;

public class ADWIN implements Serializable {
    private double delta;
    private int MAXBUCKETS = 5;
    private int lastBucketRow = 0;
    private double total = 0.0;
    private double width = 0;
    private double variance = 0.0;
    private int count = 0;
    private ListRow head;
    private ListRow tail;

    public ADWIN(double delta) {
        this.delta = delta;
        this.head = new ListRow(0);
        this.tail = this.head;
    }

    public boolean setInput(double value) {
        count++;
        insertElement(value);
        boolean drift = false;

        // Verificăm dacă trebuie să tăiem fereastra (Drift Detection)
        if (count % 32 == 0 && width > 10) {
            boolean reduce = true;
            while (reduce) {
                reduce = false;
                ListRow cursor = head;
                double n0 = 0, n1 = width;
                double u0 = 0, u1 = total;

                while (cursor != null) {
                    for (int i = 0; i < cursor.bucketCount; i++) {
                        long bucketSize = 1L << cursor.rowId;
                        n0 += bucketSize;
                        n1 -= bucketSize;
                        u0 += cursor.bucketSums[i];
                        u1 -= cursor.bucketSums[i];

                        if (n1 > 0 && n0 > 5 && n1 > 5) {
                            double diff = Math.abs((u0 / n0) - (u1 / n1));
                            double m = (1.0 / n0) + (1.0 / n1);
                            double epsilon = Math.sqrt(2 * m * (variance / width) * Math.log(2 / delta)) + (2.0 / 3.0) * m * Math.log(2 / delta);

                            if (diff > epsilon) {
                                drift = true;
                                reduce = true;
                                deleteOldestBucket();
                                break;
                            }
                        }
                    }
                    if (reduce) break;
                    cursor = cursor.next;
                }
            }
        }
        return drift;
    }

    private void insertElement(double value) {
        width++;
        total += value;
        variance += (value - getEstimation()) * (value - getEstimation());
        head.insertBucket(value);
        compress();
    }

    private void compress() {
        ListRow cursor = head;
        while (cursor != null) {
            if (cursor.bucketCount > MAXBUCKETS) {
                if (cursor.next == null) {
                    cursor.next = new ListRow(cursor.rowId + 1);
                    cursor.next.prev = cursor;
                    tail = cursor.next;
                }
                // Combinăm cele mai vechi două găleți în rândul următor
                double combinedSum = cursor.bucketSums[0] + cursor.bucketSums[1];
                cursor.removeTwoOldest();
                cursor.next.insertBucket(combinedSum);
                cursor = cursor.next;
            } else {
                break;
            }
        }
    }

    private void deleteOldestBucket() {
        ListRow cursor = tail;
        while (cursor != null && cursor.bucketCount == 0) {
            cursor = cursor.prev;
        }
        if (cursor != null) {
            long bucketSize = 1L << cursor.rowId;
            width -= bucketSize;
            total -= cursor.bucketSums[0];
            cursor.removeOldest();
        }
    }

    public double getEstimation() {
        return width > 0 ? total / width : 0;
    }

    public void setDelta(double delta) {
        this.delta = delta;
    }

    // --- STRUCTURI INTERNE PENTRU LISTĂ ---
    private class ListRow implements Serializable {
        int rowId;
        int bucketCount = 0;
        double[] bucketSums = new double[MAXBUCKETS + 1];
        ListRow next, prev;

        ListRow(int id) { this.rowId = id; }

        void insertBucket(double sum) {
            bucketSums[bucketCount++] = sum;
        }

        void removeOldest() {
            for (int i = 0; i < bucketCount - 1; i++) bucketSums[i] = bucketSums[i + 1];
            bucketCount--;
        }

        void removeTwoOldest() {
            removeOldest();
            removeOldest();
        }
    }
}