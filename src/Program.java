import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import net.openhft.hashing.LongHashFunction;

public class Program {

	static ConcurrentMap<Path, Long> name_key = new ConcurrentHashMap<>();

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		long StartTime = System.currentTimeMillis();
		String dir = "C:\\Users\\liran\\Desktop\\google-images-download\\google_images_download\\downloads\\Apple and Banana and Elephant";
		name_key = Files.walk(Paths.get(dir)).parallel().filter(f -> f.toFile().isFile())
				.filter(f -> f.toString().endsWith("jpg") || f.toString().endsWith("jpeg")
						|| f.toString().endsWith("gif") || f.toString().endsWith("png"))
				.collect(Collectors.toConcurrentMap(Path::toAbsolutePath, p -> GetHashImage(p)));

		List<Long> list_of_all_keys_more_than_one = name_key.values().parallelStream()
				.collect(Collectors.groupingBy(Function.identity(), Collectors.counting())).entrySet().parallelStream()
				.filter(v -> v.getValue() > 1).map(Map.Entry::getKey).collect(Collectors.toList());

		System.out.println("list of all keys more than one");
		list_of_all_keys_more_than_one.parallelStream().forEach(System.out::println);

		List<Path> pathtoDeleted = new ArrayList<>();

		list_of_all_keys_more_than_one.parallelStream().forEach(key -> {

			pathtoDeleted.add(name_key.entrySet().parallelStream().filter(e -> e.getValue().equals(key)).findFirst()
					.map(Entry::getKey).get());

		});
		long endTime = System.currentTimeMillis();
		
		for(Path path : pathtoDeleted)
		{
			System.out.println(path.getFileName() + " Deleted");
			Files.deleteIfExists(path);
		}

		System.out.println("time to process " + (endTime - StartTime));

	}

	private static long GetHashImage(Path f) {

		try {
			File file = f.toFile();
			RandomAccessFile random = new RandomAccessFile(file, "r");
			byte[] b = new byte[(int) file.length()];
			random.readFully(b);
			random.close();
			return LongHashFunction.xx().hashBytes(b);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return 0;
//-7392529867172308720
//-7392529867172308720
	}

}
