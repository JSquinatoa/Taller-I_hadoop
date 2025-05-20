package com.uce.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CacaoMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable produccion = new IntWritable();
    private final Text pais = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Saltar la cabecera
        if (value.toString().startsWith("País")) return;

        String[] campos = value.toString().split(",");
        if (campos.length >= 3) {
            pais.set(campos[0]);
            try {
                int prod = Integer.parseInt(campos[2]);
                produccion.set(prod);
                context.write(pais, produccion);
            } catch (NumberFormatException e) {
                // Si el valor no es numérico, lo ignoramos
            }
        }
    }
}