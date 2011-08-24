package com.sap.utility;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Feb 21, 2011
 * Time: 11:05:47 AM
 * To change this template use File | Settings | File Templates.
 */
public class NameCollector {
    //public static final String BASE_DIR = "/usr/local/projects/sadoop/data/";
    public static final String BASE_DIR = "c:\\projects\\sadoop\\data\\";

    public static final String NAME_FILE = BASE_DIR + "names.csv";
    public static final String FRIEND_FILE = BASE_DIR + "friends.csv";
    public static final String PERSON_FRIENDS_FILE = BASE_DIR + "person_friends_1m-200-20.csv";
    public static final String FRIENDS_PERSON_FILE = BASE_DIR + "friends_person_1m-50-20.csv";

    public static Collection<String> getNameList(File f) throws Exception {
        String startPrefix = "<td class=\"name\">";
        String endPrefix = "</td>";
        BufferedReader in = IOUtil.getBufferedReader(f);
        String line = in.readLine();
        Collection<String> list = new ArrayList<String>();
        while (line != null) {
            if (line.indexOf(startPrefix) >= 0) {
                String name = line.substring(line.indexOf(startPrefix) + startPrefix.length(), line.indexOf(endPrefix));
                list.add(name);
                System.out.println("name = " + name);
            }
            line = in.readLine();
        }
        in.close();
        return list;
    }

    public static Collection<String> getNames1() throws Exception {
        File folder = new File("C:\\temp\\");
        File[] files = folder.listFiles();
        System.out.println("files=" + files);
        Collection<String> allNames = new ArrayList<String>();
        for (File f : files) {
            allNames.addAll(getNameList(f));
        }

        for (String name : allNames) {
            System.out.println("name = " + name);
        }
        return allNames;
    }

    public static Collection<String> getNameListFromUrl(String url) throws Exception {
        String startPrefix = "<td class=\"name\">";
        String endPrefix = "</td>";
        BufferedReader in = IOUtil.getUrlBufferedReader(url);
        String line = in.readLine();
        Collection<String> list = new ArrayList<String>();
        while (line != null) {
            if (line.indexOf(startPrefix) >= 0) {
                String name = line.substring(line.indexOf(startPrefix) + startPrefix.length(), line.indexOf(endPrefix));
                list.add(name);
                System.out.println("name = " + name);
            }
            line = in.readLine();
        }
        in.close();
        return list;
    }

    public static Collection<String> getNames2() throws Exception {
        Collection<String> names = new ArrayList<String>();
        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-a.html"));
        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-b.html"));
        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-c.html"));
        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-d.html"));
        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-e.html"));
        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-f.html"));
        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-g.html"));
        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-h.html"));
        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-i.html"));
        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-j.html"));

        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-k.html"));
        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-l.html"));
        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-m.html"));
        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-n.html"));
        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-o.html"));
        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-p.html"));
        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-q.html"));
        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-r.html"));
        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-s.html"));
        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-t.html"));

        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-u.html"));
        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-v.html"));
        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-w.html"));
        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-x.html"));
        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-y.html"));
        names.addAll(getNameListFromUrl("http://www.momswhothink.com/baby-girl-names/baby-girl-names-z.html"));

        return names;
    }

    public static void getUniqueNames() throws Exception {
        Collection<String> names = new ArrayList<String>();
        names.addAll(getNames1());
        names.addAll(getNames2());
        Set<String> set = new LinkedHashSet<String>();
        set.addAll(names);
        BufferedWriter out = IOUtil.getBufferedWriter(NAME_FILE);
        int i = 1;
        for (String name : set) {
            out.write(i + "," + name + "\n");
            i++;
        }
        out.close();
    }

    public static Collection<String[]> getIdNameCollection() throws Exception {
        BufferedReader in = IOUtil.getBufferedReader(NAME_FILE);
        String line = in.readLine();
        Collection<String[]> names = new ArrayList<String[]>();
        while (line != null) {
            names.add(line.trim().split(","));
            line = in.readLine();
        }
        in.close();
        return names;
    }

    public static Collection<Friends> getFriendCollection() throws Exception {
        Collection<String[]> idNames = getIdNameCollection();
        Map<Integer, List<Integer>> personalFriends = new HashMap<Integer, List<Integer>>();
        List<Integer> idList = new ArrayList<Integer>();
        for (String[] idName : idNames) {
            idList.add(Integer.parseInt(idName[0]));
        }
        int index = 0;
        for (String[] idName : idNames) {
            int nowId = Integer.parseInt(idName[0]);
            // Everyone needs at least 2 friends
            int numOfFriends = (int) (200 * Math.random() + 2);
//System.out.println(index +", Working on: id="+nowId+", name="+idName[1]);
            addFriends(nowId, idList, personalFriends, numOfFriends);
            idList.add(nowId);
            index++;
        }

        Set<Friends> friendSet = new LinkedHashSet<Friends>();

        for (Map.Entry<Integer, List<Integer>> entry : personalFriends.entrySet()) {
            int myId = entry.getKey();
            List<Integer> myFriends = entry.getValue();
            for (int friendId : myFriends) {
                Friends f = new Friends(myId, friendId);
                friendSet.add(f);
            }
        }
        return friendSet;
    }


    private static void addFriends(int myId, List<Integer> idList, Map<Integer, List<Integer>> personalFriends, int numOfFriends) {
        List<Integer> myFriends = personalFriends.get(myId);
        if (myFriends == null) {
            myFriends = new ArrayList<Integer>();
        }
        int count = myFriends.size();
        while (count < numOfFriends) {
            int randomFriendIndex = (int) (Math.random() * idList.size());
            int randomFriendId = idList.get(randomFriendIndex);
            if (!myFriends.contains(randomFriendId) && myId != randomFriendId) {
                myFriends.add(randomFriendId);

                List<Integer> hisFriends = personalFriends.get(randomFriendId);
                if (hisFriends == null) {
                    hisFriends = new ArrayList<Integer>();
                }
                hisFriends.add(myId);
                personalFriends.put(randomFriendId, hisFriends);
                count++;
            }
        }
        personalFriends.put(myId, myFriends);
    }

    private static void writePersonFriends() throws Exception {
        StringBuilder sb = new StringBuilder();
        Collection<String> friendLines = IOUtil.getFileLineContent(NameCollector.FRIEND_FILE);
        sb.append("ID,FID").append("\n");
        for (String line : friendLines) {
            StringTokenizer st = new StringTokenizer(line, ",");
            String id1 = st.nextToken();
            String id2 = st.nextToken();
            sb.append(id1).append(",").append(id2).append("\n");
            sb.append(id2).append(",").append(id1).append("\n");
        }
        IOUtil.toFile(sb.toString(), PERSON_FRIENDS_FILE);
    }

    private static Collection<Integer> getIds(int size) {
        Collection<Integer> ids = new HashSet<Integer>(size);
        for (int i = 1; i <= size; i++) {
            if (i % 1000000 == 0) {
                System.out.println("i = " + i);
            }
            ids.add(i);
        }
        return ids;
    }

    private static void writeFriendCollection(int numOfPerson) throws Exception {
        Collection<Integer> ids = getIds(numOfPerson);
        Map<Integer, List<Integer>> personalFriends = new HashMap<Integer, List<Integer>>();
        List<Integer> idList = new ArrayList<Integer>();
        for (int id : ids) {
            idList.add(id);
        }
        int index = 0;
        for (int nowId : ids) {
            // Everyone needs at least 2 friends
            int numOfFriends = (int) (50 * Math.random() + 20);
            if (index % 10000 == 0) {
                System.out.println(index + ", Working on: id=" + nowId + ", and he needs " + numOfFriends + " friends.");
            }
            addFriends(nowId, idList, personalFriends, numOfFriends);
            idList.add(nowId);
            index++;
        }

        BufferedWriter out = IOUtil.getBufferedWriter(PERSON_FRIENDS_FILE);
        out.write("ID,FID\n");
        for (Map.Entry<Integer, List<Integer>> entry : personalFriends.entrySet()) {
            int nowId = entry.getKey();
            List<Integer> fids = entry.getValue();
            for (int fid : fids) {
                StringBuilder sb = new StringBuilder();
                sb.append(nowId).append(",").append(fid).append("\n");
                out.write(sb.toString());
            }
            fids = null;
        }
        out.close();
    }

    public static void main(String[] arg) throws Exception {
        writePersonFriends();
    }
}