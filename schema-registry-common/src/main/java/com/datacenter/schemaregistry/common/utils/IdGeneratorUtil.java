package com.datacenter.schemaregistry.common.utils;

import java.util.Random;

/**
 * From<br>
 * 
 * <pre>
 * https://github.com/twitter/snowflake/blob/master/src/main/scala/com/twitter/service/snowflake/SelfIdGeneratorUtil.scala
 * </pre>
 */
public class IdGeneratorUtil {

	private final long workerId;
	private final long datacenterId;
	private final long idepoch;

	private static final long workerIdBits = 5L;
	private static final long datacenterIdBits = 5L;
	private static final long maxWorkerId = -1L ^ (-1L << workerIdBits);
	private static final long maxDatacenterId = -1L ^ (-1L << datacenterIdBits);

	private static final long sequenceBits = 12L;
	private static final long workerIdShift = sequenceBits;
	private static final long datacenterIdShift = sequenceBits + workerIdBits;
	private static final long timestampLeftShift = sequenceBits + workerIdBits + datacenterIdBits;
	private static final long sequenceMask = -1L ^ (-1L << sequenceBits);

	private long lastTimestamp = -1L;
	private long sequence;
	private static final Random r = new Random();

	public static long generatId() {
		return ID_UTIL.getId();
	}

	private IdGeneratorUtil() {
		this(1451577600000L);
	}

	private static final IdGeneratorUtil ID_UTIL = new IdGeneratorUtil();

	private IdGeneratorUtil(long idepoch) {
		this(r.nextInt((int) maxWorkerId), r.nextInt((int) maxDatacenterId), 0, idepoch);
	}

	@SuppressWarnings("unused")
	private IdGeneratorUtil(long workerId, long datacenterId, long sequence) {
		this(workerId, datacenterId, sequence, 1344322705519L);
	}

	public IdGeneratorUtil(long workerId, long datacenterId, long sequence, long idepoch) {
		this.workerId = workerId;
		this.datacenterId = datacenterId;
		this.sequence = sequence;
		this.idepoch = idepoch;
		if (workerId < 0 || workerId > maxWorkerId) {
			throw new IllegalArgumentException("WorkerId is illegal: " + workerId);
		}
		if (datacenterId < 0 || datacenterId > maxDatacenterId) {
			throw new IllegalArgumentException("DatacenterId is illegal: " + workerId);
		}
		if (idepoch >= System.currentTimeMillis()) {
			throw new IllegalArgumentException("Idepoch is illegal: " + idepoch);
		}
	}

	public long getDatacenterId() {
		return datacenterId;
	}

	public long getWorkerId() {
		return workerId;
	}

	public long getTime() {
		return System.currentTimeMillis();
	}

	public long getId() {
		long id = nextId();
		return id;
	}

	private synchronized long nextId() {
		long timestamp = timeGen();
		if (timestamp < lastTimestamp) {
			throw new IllegalStateException("Clock moved backwards.");
		}
		if (lastTimestamp == timestamp) {
			sequence = (sequence + 1) & sequenceMask;
			if (sequence == 0) {
				timestamp = tilNextMillis(lastTimestamp);
			}
		} else {
			sequence = 0;
		}
		lastTimestamp = timestamp;
		long id = ((timestamp - idepoch) << timestampLeftShift)/**/
				| (datacenterId << datacenterIdShift)/**/
				| (workerId << workerIdShift)/**/
				| sequence;
		return id;
	}

	private long tilNextMillis(long lastTimestamp) {
		long timestamp = timeGen();
		while (timestamp <= lastTimestamp) {
			timestamp = timeGen();
		}
		return timestamp;
	}

	private long timeGen() {
		return System.currentTimeMillis();
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder();
		builder.append("SelfIdGeneratorUtil{");
		builder.append("workerId=").append(workerId);
		builder.append(", datacenterId=").append(datacenterId);
		builder.append(", idepoch=").append(idepoch);
		builder.append(", lastTimestamp=").append(lastTimestamp);
		builder.append(", sequence=").append(sequence);
		builder.append("}");
		return builder.toString();
	}

}