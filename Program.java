import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import net.openhft.hashing.LongHashFunction;

public class Program {

	static ConcurrentMap<Path, Long> name_key = new ConcurrentHashMap<>();

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		long StartTime = System.nanoTime();
		String dir = "B:\\All_Data_To_Scan";

		name_key = BuildMyMapInformation(dir);

		List<Long> list_of_all_keys_more_than_one = Get_All_keys_That_Have_more_than_one();

		System.out.println("list of all keys more than one");

		list_of_all_keys_more_than_one.parallelStream().forEach(System.out::println);

		List<Path> pathtoDeleted = new ArrayList<Path>();

		Get_All_Path_need_to_deleted(list_of_all_keys_more_than_one, pathtoDeleted);

		Delete_All_Path(pathtoDeleted);

		long endTime = System.nanoTime();

		System.out.println("time to process " + ((endTime - StartTime)/ 1.0 + " ms"));

	}

	private static void Delete_All_Path(List<Path> pathtoDeleted) throws IOException {
		// TODO Auto-generated method stub
		for (Path path : pathtoDeleted) {
			System.out.println(path.getFileName() + " Deleted");
			Files.deleteIfExists(path);

		}

	}

	private static void Get_All_Path_need_to_deleted(List<Long> list_of_all_keys_more_than_one,
			List<Path> pathtoDeleted2) {
		// TODO Auto-generated method stub

		list_of_all_keys_more_than_one.parallelStream().forEach(key -> {

			pathtoDeleted2.add(name_key.entrySet().parallelStream().filter(e -> e.getValue().equals(key)).findFirst()
					.map(Entry::getKey).get());

		});

	}

	private static List<Long> Get_All_keys_That_Have_more_than_one() {
		// TODO Auto-generated method stub
		return name_key.values().parallelStream().filter(v -> v != 0)
				.collect(Collectors.groupingBy(Function.identity(), Collectors.counting())).entrySet().parallelStream()
				.filter(v -> v.getValue() > 1).map(Map.Entry::getKey).collect(Collectors.toList());
	}

	private static ConcurrentMap<Path, Long> BuildMyMapInformation(String dir) throws IOException {
		// TODO Auto-generated method stub
		return Files.walk(Paths.get(dir)).parallel().filter(f -> f.toFile().isFile())
				.filter(f -> f.toString().endsWith("jpg") || f.toString().endsWith("jpeg")
						|| f.toString().endsWith("gif") || f.toString().endsWith("png") || f.toString().endsWith("JPG"))
				.collect(Collectors.toConcurrentMap(Path::toAbsolutePath, p -> GetHashImage_Sample(p)));
	}

	private static long GetHashImage_Sample(Path f) {
		try {
			File file = f.toFile();
			RandomAccessFile random = new RandomAccessFile(file, "r");
			byte[] b = new byte[1024];
			random.readFully(b);
			random.close();
			return LongHashFunction.xx().hashBytes(b);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
//-7392529867172308720
//-7392529867172308720
	}

}
