import java.util.ArrayList;
import java.util.List;

public class nGram {
	public static List<String> ngrams(int n, String str1) {
		String str = str1.replaceAll("\\p{Punct}+", "");;
        List<String> ngrams = new ArrayList<String>();
        String[] words = str.split(" ");
        for (int i = 0; i < words.length - n + 1; i++)
            ngrams.add(concat(words, i, i+n));
        return ngrams;
    }

    public static String concat(String[] words, int start, int end) {
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < end; i++)
            sb.append((i > start ? " " : "") + words[i]);
        return sb.toString();
    }
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		for (int n = 4; n <= 4; n++) {
            for (String ngram : ngrams(n, "This is my car. This is my car. This's is my car. This is my car. This is my car."))
                System.out.println(ngram);
            System.out.println();
        }
	}

}
