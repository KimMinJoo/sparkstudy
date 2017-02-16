/*
 *@(#)Driver.java 2017.02.15
 *
 * Copyright 2017 NHN Corp. All rights Reserved.
 * NHN PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.naver.spark;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.naver.spark.Airline.model.AirlinePerformanceParser;

/**
 * spark 프로그램을 돌릴 Driver클래스
 *
 * @author kim.minjoo
 */
public class Driver {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("My App");
		JavaSparkContext sc = new JavaSparkContext(conf);

		//입력파일 불러오기
		JavaRDD<String> inputFileRdd = sc.textFile("/AirlineData/*.csv");

		//입력파일 모델로 변환.
		JavaRDD<AirlinePerformanceParser> airlinePerformanceParserJavaRDD = inputFileRdd.map(
				new Function<String, AirlinePerformanceParser>() {
					@Override
					public AirlinePerformanceParser call(String line) throws Exception {
						return new AirlinePerformanceParser(line);
					}
				});

		JavaRDD<AirlinePerformanceParser> departureRDD = airlinePerformanceParserJavaRDD.filter(
				new Function<AirlinePerformanceParser, Boolean>() {
					@Override
					public Boolean call(AirlinePerformanceParser airlinePerformanceParser) throws Exception {
						return airlinePerformanceParser.isDepartureDelayAvailable();
					}
				});

		JavaPairRDD<String, Integer> countRDD = departureRDD.mapToPair(
				new PairFunction<AirlinePerformanceParser, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(AirlinePerformanceParser parser) throws
							Exception {
						return new Tuple2<>(parser.getYear() + " " + parser.getMonth(), 1);
					}
				});
		JavaPairRDD<String, Integer> resultRDD = countRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer integer, Integer integer2) throws Exception {
				return integer + integer2;
			}
		});

		resultRDD.saveAsTextFile("/AirlineOutputData/");
	}
}