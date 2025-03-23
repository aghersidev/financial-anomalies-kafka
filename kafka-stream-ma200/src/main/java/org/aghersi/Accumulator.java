package org.aghersi;

import java.io.Serializable;
import java.util.Deque;
import java.util.LinkedList;

public class Accumulator implements Serializable {
    private static final int WINDOW_SIZE = 200;  // Tamaño de la ventana
    private final Deque<Double> values = new LinkedList<>();  // Usar Deque para mejorar el rendimiento

    public Accumulator() {
    }
    // Agregar un valor al acumulador y devolver el mismo objeto para encadenar métodos
    public Accumulator add(double value) {
        values.add(value);
        if (values.size() > WINDOW_SIZE) {
            values.removeFirst();  // Mantener solo los últimos WINDOW_SIZE elementos
        }
        return this;  // Devolver el objeto actual (para encadenar métodos)
    }

    // Verificar si la ventana está llena
    public boolean isReady() {
        return values.size() == WINDOW_SIZE;
    }

    // Calcular la media de los valores en la ventana
    public double getMean() {
        return values.stream().mapToDouble(Double::doubleValue).average().orElse(0);
    }

    // Calcular la desviación estándar (usando el denominador adecuado)
    public double getStdDev() {
        double mean = getMean();
        return Math.sqrt(values.stream().mapToDouble(v -> Math.pow(v - mean, 2)).sum() / (WINDOW_SIZE));
    }

    // Obtener el último valor de la ventana
    public double getLast() {
        return values.isEmpty() ? 0 : values.getLast();  // Verificar si la lista está vacía
    }
}
