import java.util.StringTokenizer;


public class REgexpTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
			String text="01234gegeglŽ’—lol345 ";
			System.out.println(text);
			String line = text.replaceAll("[^\\p{ASCII}]|\\d", "");
			System.out.println(line);
			System.out.println(line.replaceAll("\\d",""));
			StringTokenizer itr = new StringTokenizer(line,
					"-- \t\n\r\f,.:;?![]'\"#()$%&@*+-/");
	}

}
