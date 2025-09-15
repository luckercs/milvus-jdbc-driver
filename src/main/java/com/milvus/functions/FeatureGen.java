package com.milvus.functions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class FeatureGen {

    private List<Float> gen_one_random_float_vector_list(int dim) {
        Random random = new Random();
        List<Float> vec = new ArrayList<>();
        for (int k = 0; k < dim; k++) {
            vec.add(random.nextFloat());
        }
        return vec;
    }

    public List<List<Float>> gen_random_float_vectors_list(int dim, int num) {
        List<List<Float>> vectorList = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            List<Float> vec = gen_one_random_float_vector_list(dim);
            vectorList.add(vec);
        }
        return vectorList;
    }

    public String gen_random_float_vectors_str(int dim, int num) {
        List<List<Float>> vectorList = gen_random_float_vectors_list(dim, num);
        String vecStr = vectorList.toString();
        return vecStr;
    }


    private ByteBuffer gen_one_random_binary_vector(int dim) {
        Random ran = new Random();
        int byteCount = dim / 8;
        ByteBuffer vector = ByteBuffer.allocate(byteCount);
        for (int i = 0; i < byteCount; ++i) {
            vector.put((byte) ran.nextInt(Byte.MAX_VALUE));
        }
        return vector;
    }

    private List<Byte> gen_one_random_binary_vector_list(int dim) {
        Random ran = new Random();
        int byteCount = dim / 8;
        ByteBuffer vector = ByteBuffer.allocate(byteCount);
        for (int i = 0; i < byteCount; ++i) {
            vector.put((byte) ran.nextInt(Byte.MAX_VALUE));
        }
        vector.flip();

        List<Byte> vecList = new ArrayList<>();
        while (vector.hasRemaining()) {
            vecList.add(vector.get());
        }
        return vecList;
    }

    private List<ByteBuffer> gen_random_binary_vectors_bytebuffer_list(int dim, int num) {
        List<ByteBuffer> vectorList = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            ByteBuffer byteBuffer = gen_one_random_binary_vector(dim);
            vectorList.add(byteBuffer);
        }
        return vectorList;
    }

    public List<List<Byte>> gen_random_binary_vectors_list(int dim, int num) {
        List<List<Byte>> vectorList = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            List<Byte> vec = gen_one_random_binary_vector_list(dim);
            vectorList.add(vec);
        }
        return vectorList;
    }

    public String gen_random_binary_vectors_str(int dim, int num) {
        List<List<Byte>> vecs = gen_random_binary_vectors_list(dim, num);
        String vecStr = vecs.toString();
        return vecStr;
    }

    public static void main(String[] args) {
        String s = new FeatureGen().gen_random_float_vectors_str(128, 1);
        System.out.println(s);
    }
}
