package org.opencloudb.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class SerializeUtil {

	/**
	 * 序列化对象为字节数组
	 * 
	 * @param object
	 * @return
	 */
	public static <T> byte[] serialize(T object) {
		try {
			if (!(object instanceof Serializable)) {
				throw new IllegalArgumentException(" requires a Serializable payload " + "but received an object of type [" + object.getClass().getName() + "]");
			}
			ByteArrayOutputStream byteStream = new ByteArrayOutputStream(128);
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteStream);
			objectOutputStream.writeObject(object);
			objectOutputStream.flush();
			return byteStream.toByteArray();
		} catch (Exception e) {
			throw new RuntimeException("Cannot serialize", e);
		}
		// return EMPTY_ARRAY;
	}

	/**
	 * 反序列化字节数组到对象
	 * 
	 * @param bytes
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> T deserialize(byte[] bytes) {
		if (null == bytes || bytes.length == 0) {
			return null;
		}
		try {
			ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
			ObjectInputStream objectInputStream = new ObjectInputStream(byteStream);
			return (T) objectInputStream.readObject();
		} catch (Exception e) {
			throw new RuntimeException("Cannot deserialize", e);
		}
	}

}
