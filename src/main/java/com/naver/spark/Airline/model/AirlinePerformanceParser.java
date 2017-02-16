/*
 *@(#)AirlinePerformanceParser.java 2017.02.13
 *
 * Copyright 2017 NHN Corp. All rights Reserved.
 * NHN PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.naver.spark.Airline.model;

import java.io.Serializable;

import org.apache.hadoop.io.Text;

/**
 *
 *
 * @author kim.minjoo
 */
public class AirlinePerformanceParser implements Serializable {
	private int year;
	private int month;

	private int arriveDelayTime = 0;
	private int departureDelayTime = 0;
	private int distance = 0;

	private boolean arriveDelayAvailable = true;
	private boolean departureDelayAvailable = true;
	private boolean distanceAvailable = true;

	private String uniqueCarrier;

	public AirlinePerformanceParser(String line) {
		try {
			String[] columns = line.split(",");
			//운항 연도 설정
			year = Integer.parseInt(columns[0]);
			//운항 월 설정
			month = Integer.parseInt(columns[1]);
			//항공사 코드 설정
			uniqueCarrier = columns[8];

			//항공기 출발 지연 시간 설정
			if (!"NA".equals(columns[15])) {
				departureDelayTime = Integer.parseInt(columns[15]);
			} else {
				departureDelayAvailable = false;
			}

			//항공기 도착 지연 시간 설정
			if (!"NA".equals(columns[14])) {
				arriveDelayTime = Integer.parseInt(columns[14]);
			} else {
				arriveDelayAvailable = false;
			}

			if (!"NA".equals(columns[18])) {
				distance = Integer.parseInt(columns[18]);
			} else {
				distanceAvailable = false;
			}
		} catch (Exception e) {
			System.err.println("Error parsing a record :" + e.getMessage());
		}
	}

	public int getYear() {
		return year;
	}

	public int getMonth() {
		return month;
	}

	public int getArriveDelayTime() {
		return arriveDelayTime;
	}

	public int getDepartureDelayTime() {
		return departureDelayTime;
	}

	public int getDistance() {
		return distance;
	}

	public boolean isArriveDelayAvailable() {
		return arriveDelayAvailable;
	}

	public boolean isDepartureDelayAvailable() {
		return departureDelayAvailable;
	}

	public boolean isDistanceAvailable() {
		return distanceAvailable;
	}

	public String getUniqueCarrier() {
		return uniqueCarrier;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public void setArriveDelayTime(int arriveDelayTime) {
		this.arriveDelayTime = arriveDelayTime;
	}

	public void setDepartureDelayTime(int departureDelayTime) {
		this.departureDelayTime = departureDelayTime;
	}

	public void setDistance(int distance) {
		this.distance = distance;
	}

	public void setArriveDelayAvailable(boolean arriveDelayAvailable) {
		this.arriveDelayAvailable = arriveDelayAvailable;
	}

	public void setDepartureDelayAvailable(boolean departureDelayAvailable) {
		this.departureDelayAvailable = departureDelayAvailable;
	}

	public void setDistanceAvailable(boolean distanceAvailable) {
		this.distanceAvailable = distanceAvailable;
	}

	public void setUniqueCarrier(String uniqueCarrier) {
		this.uniqueCarrier = uniqueCarrier;
	}
}