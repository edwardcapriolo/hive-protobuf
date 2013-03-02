package com.m6d.hive.protobuf;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

import org.apache.hadoop.io.BytesWritable;

import prototest.Ex;

public class ProtobufDeserializerBench {
	
	public static Ex.Garage garage(String address, Ex.Car... cars) {
		return Ex.Garage.newBuilder()
				.setAddress(address)
				.addAllCars(Arrays.asList(cars))
				.build();
	}
	
	public static Ex.Car car(Ex.Person owner) {
		return Ex.Car.newBuilder()
				.setOwner(owner)
				.addAllTires(Arrays.asList(tire, tire, tire, tire))
				.build();
	}
	
	public static Ex.Person person(int id, String name) {
		return Ex.Person.newBuilder()
				.setId(id)
				.setName(name)
				.build();
	}
	
	public static final Ex.TireMaker tireMaker = Ex.TireMaker.newBuilder()
		.setMaker("Goodyear")
		.setPrice(99)
		.build();
	
	public static final Ex.Tire tire = Ex.Tire.newBuilder()
		.setTireMaker(tireMaker)
		.setTirePressure(60)
		.build();
	
	public static BytesWritable mkRow(int id) throws Exception {
		Ex.Garage garage = garage("30 Broad Street",
			car(person(0, "Koons")),
			car(person(1, "Lance")),
			car(person(2, "Paul")),
			car(person(3, "Jimmie")),
			car(person(4, "Zed")),
			car(person(5, "Maynard")),
			car(person(6, "Ringo")),
			car(person(7, "Yolanda")),
			car(person(8, "Vincent")),
			car(person(9, "Jules")),
			car(person(10, "Mia")),
			car(person(11, "Marsellus")),
			car(person(12, "Butch")),
			car(person(13, "Fabienne")),
			car(person(14, "Jody")),
			car(person(15, "Marvin")),
			car(person(16, "Winston")),
			car(person(17, "Brett"))
		);

		BytesWritable value = new BytesWritable();

		ByteArrayOutputStream s = new ByteArrayOutputStream();
		garage.writeTo(s);
		value.set(s.toByteArray(), 0, s.toByteArray().length);
		return value;
	}
	
	public static void main(String[] args) throws Exception {
		ProtobufDeserializer de = new ProtobufDeserializer();
		de.valueClass = Ex.Garage.class;
		de.vparseFrom = de.valueClass.getMethod("parseFrom", de.byteArrayParameters);
		de.buildObjectInspector();
		
		int warmup = 10001;
		for(int i = 0; i<warmup; i++) {
			BytesWritable val = mkRow(i);
			Object result = de.deserialize(val);
		}
		
		int iters = 100000;
		long start = System.currentTimeMillis();
		for(int i = 0; i<iters; i++) {
			BytesWritable val = mkRow(i);
			Object result = de.deserialize(val);
		}
		long end = System.currentTimeMillis();
		System.out.println("Elapsed: " + (end - start));
	}

}
