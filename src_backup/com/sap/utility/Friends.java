package com.sap.utility;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Feb 21, 2011
 * Time: 3:31:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class Friends {
    private int id1;
    private int id2;

    public Friends(int id1, int id2) {
        this.id1 = id1;
        this.id2 = id2;
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Friends friends = (Friends) o;

        if (id1 == friends.id1 && id2 == friends.id2) return true;
        if (id1 == friends.id2 && id2 == friends.id1) return true;

        return false;
    }

    public int hashCode() {
        return Math.abs(id1 * id1 - id2 * id2);
    }

    public int getId1() {
        return id1;
    }

    public int getId2() {
        return id2;
    }
}
