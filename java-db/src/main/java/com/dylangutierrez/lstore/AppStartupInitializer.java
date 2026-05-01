package com.dylangutierrez.lstore;

import com.dylangutierrez.lstore.dataset.DatasetBootstrap;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * Ensures the read-only Courses dataset exists before the application begins
 * serving requests, then warms the in-memory cache used by the API layer.
 */
@Component
public class AppStartupInitializer implements ApplicationRunner {

    private final CoursesDataService coursesDataService;

    public AppStartupInitializer(CoursesDataService coursesDataService) {
        this.coursesDataService = coursesDataService;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        long totalStart = System.currentTimeMillis();

        System.out.println("Ensuring Courses dataset exists...");
        long datasetStart = System.currentTimeMillis();

        // Build the dataset only if it is missing. Do not force a rebuild.
        DatasetBootstrap.ensureCoursesLoaded(false);

        long datasetElapsed = System.currentTimeMillis() - datasetStart;
        System.out.println("Courses dataset is ready in " + datasetElapsed + " ms.");

        // Warm the API cache only after the dataset exists on disk.
        coursesDataService.warmCache();

        long totalElapsed = System.currentTimeMillis() - totalStart;
        System.out.println("Application data initialization complete in " + totalElapsed + " ms.");
    }
}