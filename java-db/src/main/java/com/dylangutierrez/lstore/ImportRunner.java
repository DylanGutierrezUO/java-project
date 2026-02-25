package com.dylangutierrez.lstore;

import com.dylangutierrez.lstore.dataset.DatasetBootstrap;

/**
 * One-time runner to import src/main/resources/data.csv into the database.
 *
 * Usage (PowerShell):
 *   mvn -DskipTests compile
 *   java -cp "target/classes" com.dylangutierrez.lstore.ImportRunner
 *
 * Force re-import (deletes existing data/Courses first):
 *   java -cp "target/classes" com.dylangutierrez.lstore.ImportRunner --force
 */
public final class ImportRunner {

    private ImportRunner() {
        // Prevent instantiation.
    }

    public static void main(String[] args) {
        try {
            boolean force = hasFlag(args, "--force") || hasFlag(args, "--reimport");
            if (hasFlag(args, "--help") || hasFlag(args, "-h")) {
                printUsage();
                return;
            }

            if (force) {
                System.out.println("Forcing Courses re-import (existing on-disk table will be replaced)...");
            }

            DatasetBootstrap.ensureCoursesLoaded(force);
            System.out.println("Courses dataset ready.");
        } catch (Exception e) {
            System.err.println("CSV import failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static boolean hasFlag(String[] args, String flag) {
        if (args == null || flag == null) {
            return false;
        }
        for (String a : args) {
            if (flag.equalsIgnoreCase(a)) {
                return true;
            }
        }
        return false;
    }

    private static void printUsage() {
        System.out.println("ImportRunner usage:");
        System.out.println("  java -cp target/classes com.dylangutierrez.lstore.ImportRunner");
        System.out.println("  java -cp target/classes com.dylangutierrez.lstore.ImportRunner --force");
        System.out.println();
        System.out.println("Notes:");
        System.out.println("  - Uses src/main/resources/data.csv as the input.");
        System.out.println("  - Uses -Dlstore.data_dir=<path> to control where the DB writes data.");
    }
}