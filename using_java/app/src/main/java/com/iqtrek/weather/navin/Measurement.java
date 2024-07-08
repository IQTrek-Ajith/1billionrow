package com.iqtrek.weather.navin;

import lombok.Data;

@Data
class Measurement {
        private Double min = Double.MAX_VALUE;
        private Double max = Double.MIN_VALUE;
        private Double sum;
        private Integer count;
        private String name;

    public Measurement(String name, Double temperature) {
        this.min = temperature;
        this.max = temperature;
        this.sum = temperature;
        this.count = 1;
        this.name = name;
    }

    public String toString() {
        return String.format("%s, %s, %s, %s",
                round(min), round(sum/ count), round(max), count);

    }
    public static Measurement parseLine(String line) {
        String[] parts = line.split(";");
        if (parts.length != 2) {
            return new Measurement("error", 0D);
        }
        String name = parts[0];
        Double temperature = Double.parseDouble(parts[1]);
        return new Measurement(name, temperature);
    }
    public Measurement aggregate(Measurement target) {
        target.setMin(Math.min(this.getMin(), target.getMin()));
        target.setMax(Math.max(this.getMax(), target.getMax()));
        target.setSum(this.getSum() + target.getSum());
        target.setCount(this.getCount() + target.getCount());
        return target;
    }
    private String round(Double value) {
        return String.format("%.2f", value);
    }


}