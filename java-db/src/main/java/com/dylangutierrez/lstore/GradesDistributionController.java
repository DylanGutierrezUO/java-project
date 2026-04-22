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
            @RequestParam(defaultValue = "instructor") String groupBy,
            @RequestParam(defaultValue = "none") String filterType,
            @RequestParam(defaultValue = "") String filterValue
    ) {
        return coursesDataService.getDistribution(metric, groupBy, filterType, filterValue);
    }
}