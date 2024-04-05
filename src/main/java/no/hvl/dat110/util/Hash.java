package no.hvl.dat110.util;

/**
 * exercise/demo purpose in dat110
 * @author tdoy
 *
 */

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hash {

	/**
	 * Uses the MD5 algorithm to hash a given string and returns the resulting hash as a {@link BigInteger}.
	 * This method throws a {@link RuntimeException} if the MD5 algorithm is not available in the environment.
	 *
	 * @param entity The string to be hashed.
	 * @return A {@link BigInteger} representation of the MD5 hash.
	 * @throws RuntimeException if the MD5 algorithm is not found.
	 * @author Vegard
	 */
	public static BigInteger hashOf(String entity){
		
		BigInteger hashint = null;
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] messageDigest = md.digest(entity.getBytes());
			hashint = new BigInteger(1, messageDigest);
		}
		catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
		return hashint;
	}

	/**
	 *
	 * @return BigInteger the address-space of MD5 algorithm
	 * @author Vegard
	 */
	public static BigInteger addressSize() {
		return BigInteger.valueOf(2).pow(128);
	}

	/**
	 *
	 * @return digestLen * 8, bitsize of MD5 algorithm
	 * @throws RuntimeException if the algorithm isnt found
	 * @author Vegard
	 */
	public static int bitSize() {
		try {
			// Create a MessageDigest object for MD5
			MessageDigest md = MessageDigest.getInstance("MD5");
			// Get the digest length in bytes. MD5 has a fixed size of 16 bytes (128 bits).
			int digestLen = md.getDigestLength();
			// Convert length in bytes to bits (1 byte = 8 bits) and return
			return digestLen * 8;
		} catch (NoSuchAlgorithmException e) {
			// This exception should never be thrown for MD5
			throw new RuntimeException(e);
		}
	}
	
	public static String toHex(byte[] digest) {
		StringBuilder strbuilder = new StringBuilder();
		for(byte b : digest) {
			strbuilder.append(String.format("%02x", b&0xff));
		}
		return strbuilder.toString();
	}

}
