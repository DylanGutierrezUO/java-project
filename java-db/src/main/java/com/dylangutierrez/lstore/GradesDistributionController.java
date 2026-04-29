package com.dylangutierrez.lstore;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * API controller for grade distribution requests.
 */
@RestController
@RequestMapping("/api/v1/grades")
public class GradesDistributionController {

    private final CoursesDataService coursesDataService;

    public GradesDistributionController(CoursesDataService coursesDataService) {
        this.coursesDataService = coursesDataService;
    }

    @GetMapping("/distribution")
    public CoursesDataService.DistributionData getDistribution(
            @RequestParam(defaultValue = "A") String metric,
            @RequestParam(defaultValue = "ALL") String department,
            @RequestParam(defaultValue = "ALL") String course
    ) {
        return coursesDataService.getDistribution(metric, department, course);
    }

    @GetMapping("/options")
    public CoursesDataService.OptionsData getOptions() {
        return coursesDataService.getOptions();
    }
}