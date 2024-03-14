package csv2json;

import java.util.HashMap;
import java.util.Map;

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