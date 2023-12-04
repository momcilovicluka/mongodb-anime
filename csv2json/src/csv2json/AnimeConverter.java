package csv2json;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import com.opencsv.exceptions.CsvValidationException;

public class AnimeConverter {

	private static final String unknown = "Unknown";

	public static void main(String[] args) throws CsvException {
		try {
			String animeCsvFilePath = "/home/luka/Documents/NoSQLProjekat/anime/anime.csv";
			String animeJsonFilePath = "/home/luka/Documents/NoSQLProjekat/anime/nosqlAnimeSynopsis.json";

			System.out.println("CSV input: " + animeCsvFilePath);
			System.out.println("JSON output: " + animeJsonFilePath);

			System.out.println("🔁Commencing conversion to JSON...");
			convertAnimeCsvToJson(animeCsvFilePath, animeJsonFilePath);
			System.out.println("✅Conversion finished.");

			String ratingsCsvFilePath = "/home/luka/Documents/NoSQLProjekat/anime/animelist.csv";
			String synopsisCsvFilePath = "/home/luka/Documents/NoSQLProjekat/anime/anime_with_synopsis.csv";
			System.out.println("Ratings CSV: " + ratingsCsvFilePath);
			System.out.println("Synopsis CSV: " + synopsisCsvFilePath);

			System.out.println("ℹCommencing adding additional info...");
			addAdditionalInfoToAnimeJson(animeJsonFilePath, ratingsCsvFilePath, synopsisCsvFilePath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void convertAnimeCsvToJson(String csvFilePath, String jsonFilePath)
			throws IOException, CsvException {
		CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath)).build();
		List<String[]> lines = reader.readAll();

		JSONArray animeArray = new JSONArray();
		for (int i = 1; i < lines.size(); i++) {
			System.out.println("Adding anime: " + i);
			String[] line = lines.get(i);
			JSONObject animeObject = new JSONObject();

			animeObject.put("malId", Integer.parseInt(line[0]));

			if (!line[1].equals(unknown))
				animeObject.put("name", line[1]);

			try {
				animeObject.put("score", Double.parseDouble(line[2]));
			} catch (Exception e) {
				// animeObject.put("score", line[2]);
			}

			if (!line[3].equals(unknown)) {
				String[] genres = line[3].split(", ");
				JSONArray genresArray = new JSONArray(genres);
				animeObject.put("genres", genresArray);
			}

			String englishName = line[4];
			if (!englishName.equals(unknown) && !englishName.equals("?"))
				animeObject.put("englishName", line[4]);

			String japaneseName = line[5];
			if (!japaneseName.equals(unknown) && !japaneseName.equals("?"))
				animeObject.put("japaneseName", line[5]);

			if (!line[6].equals(unknown))
				animeObject.put("type", line[6]);

			try {
				animeObject.put("episodes", Integer.parseInt(line[7]));
			} catch (Exception e) {
				// animeObject.put("episodes", line[7]);
			}

			String airedString = line[8];
			String[] airedDates = parseAiredDates(airedString);
			if (airedDates[0] != null && !airedDates[0].equals("?") && !airedDates[0].equals(unknown)
					&& airedDates[0].isBlank())
				animeObject.put("airedStart", airedDates[0]);
			if (airedDates[1] != null && !airedDates[1].equals("?") && !airedDates[1].equals(unknown)
					&& airedDates[1].isBlank())
				animeObject.put("airedEnd", airedDates[1]);

			String premiered = line[9];
			if (!premiered.equals(unknown)) {
				JSONObject premieredObject = new JSONObject();
				premieredObject.put("season", premiered.split(" ")[0]);
				premieredObject.put("year", Integer.parseInt(premiered.split(" ")[1]));
				animeObject.put("premiered", premieredObject);
			}

			if (!line[10].equals(unknown)) {
				String[] producers = line[10].split(", ");
				JSONArray producersArray = new JSONArray(producers);
				animeObject.put("producers", producersArray);
			}

			if (!line[11].equals(unknown)) {
				String[] licensors = line[11].split(", ");
				JSONArray licensorsArray = new JSONArray(licensors);
				animeObject.put("licensors", licensorsArray);
			}

			if (!line[12].equals(unknown)) {
				String[] studios = line[12].split(", ");
				JSONArray studiosArray = new JSONArray(studios);
				animeObject.put("studios", studiosArray);
			}

			if (!line[13].equals(unknown))
				animeObject.put("source", line[13]);

			if (!line[14].equals(unknown))
				animeObject.put("duration", convertToMinutes(line[14]));

			if (!line[15].equals(unknown))
				animeObject.put("rating", line[15]);

			if (!line[16].equals(unknown))
				animeObject.put("ranked", Integer.parseInt(line[16].split("\\.")[0]));

			if (!line[17].equals(unknown))
				animeObject.put("popularity", Integer.parseInt(line[17]));

			if (!line[18].equals(unknown))
				animeObject.put("members", Integer.parseInt(line[18]));

			if (!line[19].equals(unknown))
				animeObject.put("favorites", Integer.parseInt(line[19]));

			JSONObject watchStatisticObject = new JSONObject();
			watchStatisticObject.put("watching", Integer.parseInt(line[20]));
			watchStatisticObject.put("completed", Integer.parseInt(line[21]));
			watchStatisticObject.put("on-hold", Integer.parseInt(line[22]));
			watchStatisticObject.put("dropped", Integer.parseInt(line[23]));
			watchStatisticObject.put("planToWatch", Integer.parseInt(line[24]));
			animeObject.put("watchStatistic", watchStatisticObject);

			JSONObject scoreStatisticObject = new JSONObject();
			if (!line[25].equals(unknown))
				scoreStatisticObject.put("10", Integer.parseInt(line[25].split("\\.")[0]));
			if (!line[26].equals(unknown))
				scoreStatisticObject.put("9", Integer.parseInt(line[26].split("\\.")[0]));
			if (!line[27].equals(unknown))
				scoreStatisticObject.put("8", Integer.parseInt(line[27].split("\\.")[0]));
			if (!line[28].equals(unknown))
				scoreStatisticObject.put("7", Integer.parseInt(line[28].split("\\.")[0]));
			if (!line[29].equals(unknown))
				scoreStatisticObject.put("6", Integer.parseInt(line[29].split("\\.")[0]));
			if (!line[30].equals(unknown))
				scoreStatisticObject.put("5", Integer.parseInt(line[30].split("\\.")[0]));
			if (!line[31].equals(unknown))
				scoreStatisticObject.put("4", Integer.parseInt(line[31].split("\\.")[0]));
			if (!line[32].equals(unknown))
				scoreStatisticObject.put("3", Integer.parseInt(line[32].split("\\.")[0]));
			if (!line[33].equals(unknown))
				scoreStatisticObject.put("2", Integer.parseInt(line[33].split("\\.")[0]));
			if (!line[34].equals(unknown))
				scoreStatisticObject.put("1", Integer.parseInt(line[34].split("\\.")[0]));
			if (scoreStatisticObject.length() > 0)
				animeObject.put("scoreStatistic", scoreStatisticObject);

			animeArray.put(animeObject);
		}

		System.out.println("✅Finished converting.");
		System.out.println("Writing to file: " + jsonFilePath);

		try (FileWriter fileWriter = new FileWriter(jsonFilePath)) {
			fileWriter.write(animeArray.toString(2));
		}
	}

	private static int convertToMinutes(String duration) {
		int totalMinutes = 0;

		String[] parts = duration.split("\\s");
		for (int i = 0; i < parts.length; i += 2) {
			try {
				int value = Integer.parseInt(parts[i]);
				String unit = parts[i + 1];

				if (unit.equals("min.")) {
					totalMinutes += value;
				} else if (unit.equals("hr.")) {
					totalMinutes += value * 60;
				}
			} catch (NumberFormatException e) {
				if (!parts[i].equals("per")) {
					throw e;
				}
			}
		}

		return totalMinutes;
	}

	private static String formatDate(Date date) {
		if (date == null)
			return null;

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		return dateFormat.format(date);
	}

	private static String[] parseAiredDates(String airedString) {
		String[] dates = new String[2];
		SimpleDateFormat[] dateFormats = { new SimpleDateFormat("MMM d, yyyy"), new SimpleDateFormat("MMM d, yyyy"),
				new SimpleDateFormat("MMM, yyyy"), new SimpleDateFormat("yyyy") };

		String[] dateParts = airedString.split(" to ");
		Date startDate = null;
		Date endDate = null;

		for (SimpleDateFormat dateFormat : dateFormats) {
			try {
				startDate = dateFormat.parse(dateParts[0].trim());
				break;
			} catch (ParseException ignored) {
				// ignored.printStackTrace();
			}
		}

		if (dateParts.length < 2) {
			dates[0] = formatDate(startDate);
			dates[1] = dates[0];
		} else {
			for (SimpleDateFormat dateFormat : dateFormats) {
				try {
					endDate = dateFormat.parse(dateParts[1].trim());
					break;
				} catch (ParseException ignored) {
					// ignored.printStackTrace();
				}
			}
			dates[0] = formatDate(startDate);
			dates[1] = formatDate(endDate);
		}

		return dates;
	}

	private static void addAdditionalInfoToAnimeJson(String jsonFilePath, String ratingsCsvFilePath,
			String synopsisCsvFilePath) throws IOException, CsvValidationException {
		StringBuilder jsonString = new StringBuilder();
		try (BufferedReader reader = new BufferedReader(new FileReader(jsonFilePath))) {
			String line;
			while ((line = reader.readLine()) != null) {
				jsonString.append(line);
			}
		}

		JSONArray animeArray = new JSONArray(jsonString.toString());

		CSVReader reader = new CSVReaderBuilder(new FileReader(ratingsCsvFilePath)).build();
		String[] line = reader.readNext();

		int count = 0;
		while ((line = reader.readNext()) != null) {
			if (count >= 420_069)
				break;

			System.out.println("Adding rating: " + count++ + "=>" + Arrays.toString(line));
			int userId = Integer.parseInt(line[0]);
			int animeId = Integer.parseInt(line[1]);
			int rating = Integer.parseInt(line[2]);
			int watchStatusValue = Integer.parseInt(line[3]);
			int watchedEpisodes = Integer.parseInt(line[4]);

			for (int j = 0; j < animeArray.length(); j++) {
				JSONObject animeObject = animeArray.getJSONObject(j);
				if (animeObject.getInt("malId") == animeId) {

					JSONArray ratingsArray = animeObject.optJSONArray("ratings");
					if (ratingsArray == null) {
						ratingsArray = new JSONArray();
						animeObject.put("ratings", ratingsArray);
					}

					JSONObject ratingObject = new JSONObject();
					ratingObject.put("userId", userId);
					ratingObject.put("rating", rating);
					WatchStatus watchStatus = WatchStatus.valueOf(watchStatusValue);
					ratingObject.put("watchStatus", watchStatus.getDescription());
					ratingObject.put("watchedEpisodes", watchedEpisodes);

					ratingsArray.put(ratingObject);

					break;
				}
			}
		}

		System.out.println("✅Finished adding ratings.");

		System.out.println("📄Commencing adding synopsis...");

		reader = new CSVReaderBuilder(new FileReader(synopsisCsvFilePath)).build();
		line = reader.readNext();
		count = 0;
		while ((line = reader.readNext()) != null) {
			System.out.println("Adding synopsis: " + count++ + " to anime " + line[0]);

			int animeId = Integer.parseInt(line[0]);
			String synopsis = line[4];

			for (int j = 0; j < animeArray.length(); j++) {
				JSONObject animeObject = animeArray.getJSONObject(j);
				if (animeObject.getInt("malId") == animeId) {
					animeObject.put("synopsis", synopsis);

					break;
				}
			}
		}

		System.out.println("✅Finished adding additional info.");
		System.out.println("📝Writing to file and exiting...");

		try (FileWriter fileWriter = new FileWriter(jsonFilePath)) {
			fileWriter.write(animeArray.toString(2));
		}
	}

	public enum WatchStatus {
		CURRENTLY_WATCHING(1, "Currently Watching"), COMPLETED(2, "Completed"), ON_HOLD(3, "On Hold"),
		DROPPED(4, "Dropped"), PLAN_TO_WATCH(6, "Plan to Watch");

		private final int value;
		private final String description;

		WatchStatus(int value, String description) {
			this.value = value;
			this.description = description;
		}

		public int getValue() {
			return value;
		}

		public String getDescription() {
			return description;
		}

		private static final Map<Integer, WatchStatus> map = new HashMap<>();

		static {
			for (WatchStatus status : values()) {
				map.put(status.value, status);
			}
		}

		public static WatchStatus valueOf(int value) {
			return map.get(value);
		}
	}

}