package com.example.url_shortner.utils;

import com.example.url_shortner.models.LinkDocument;

import java.security.SecureRandom;
import java.time.Instant;

import static com.example.url_shortner.utils.Constants.*;

public class Utils {
    private static final SecureRandom RANDOM = new SecureRandom();

    public static String generateUniqueRandomCode() {
            return generateRandomString(CODE_LENGTH);
    }

    private static String generateRandomString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(ALPHABET.charAt(RANDOM.nextInt(ALPHABET.length())));
        }
        return sb.toString();
    }



}
