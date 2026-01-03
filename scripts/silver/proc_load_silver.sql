/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Purpose:
  - Truncates Silver tables and reloads them from Bronze.
  - Applies basic cleansing (TRIM, type normalization, safe casts, deduping).
Usage:
  EXEC silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @start_time       DATETIME2(3),
        @end_time         DATETIME2(3),
        @batch_start_time DATETIME2(3),
        @batch_end_time   DATETIME2(3);

    BEGIN TRY
        SET @batch_start_time = SYSDATETIME();

        PRINT '================================================';
        PRINT 'Loading Silver Layer (Bronze -> Silver)';
        PRINT 'Batch start: ' + CONVERT(VARCHAR(30), @batch_start_time, 121);
        PRINT '================================================';

        /* ------------------------------------------------
           TRUNCATE (facts first)
        -------------------------------------------------*/
        PRINT '>> Truncating Silver tables...';
        TRUNCATE TABLE silver.course_grades;
        TRUNCATE TABLE silver.assessment_results;
        TRUNCATE TABLE silver.advising_interactions;
        TRUNCATE TABLE silver.enrollments;
        TRUNCATE TABLE silver.survey;
        TRUNCATE TABLE silver.program_learning_outcomes;
        TRUNCATE TABLE silver.course;
        TRUNCATE TABLE silver.programs;
        TRUNCATE TABLE silver.students;
        TRUNCATE TABLE silver.staff;
        TRUNCATE TABLE silver.terms;


        /* ------------------------------------------------
           DIMENSIONS
        -------------------------------------------------*/


        /* silver.staff */
        PRINT '------------------------------------------------';
        PRINT 'Loading silver.staff';
        SET @start_time = SYSDATETIME();

        INSERT INTO silver.staff (staff_id, first_name, last_name, unit, email)
        SELECT staff_id, first_name, last_name, unit, email
        FROM (
            SELECT
                TRIM(CAST(staff_id AS NVARCHAR(50))) AS staff_id,
                SUBSTRING(staff_name,1, CHARINDEX(' ', staff_name+' ')-1) as first_name,
                SUBSTRING(staff_name,CHARINDEX(' ', staff_name+' ')+1, LEN(staff_name))AS last_name,
                NULLIF(TRIM(unit), '')      AS unit,
                CONCAT(LOWER(SUBSTRING(staff_name,1, CHARINDEX(' ', staff_name+' ')-1)),
                LOWER(SUBSTRING(staff_name,CHARINDEX(' ', staff_name+' ')+1, LEN(staff_name))),'@univ.edu') as email,
                ROW_NUMBER() OVER (
                    PARTITION BY TRIM(CAST(staff_id AS NVARCHAR(50)))
                    ORDER BY (SELECT 1)
                ) AS rn
            FROM bronze.staff
            WHERE staff_id IS NOT NULL
        ) s
        WHERE rn = 1;

        SET @end_time = SYSDATETIME();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(20)) + ' sec';

        /* silver.terms */

        PRINT '------------------------------------------------';
        PRINT 'Loading silver.terms';
        SET @start_time = SYSDATETIME();

        INSERT INTO silver.terms (term_code, term_name, start_date, end_date)
        SELECT term_code, term_name, start_date, end_date
        FROM (
            SELECT
                TRIM(CAST(term_code AS NVARCHAR(50))) AS term_code,
                NULLIF(TRIM(term_name), '')           AS term_name,
                start_date,
                end_date,
                ROW_NUMBER() OVER (
                    PARTITION BY TRIM(CAST(term_code AS NVARCHAR(50)))
                    ORDER BY (SELECT 1)
                ) AS rn
            FROM bronze.terms
            WHERE term_code IS NOT NULL
        ) t
        WHERE rn = 1;

        SET @end_time = SYSDATETIME();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(20)) + ' sec';

        /* silver.course */

        PRINT '------------------------------------------------';
        PRINT 'Loading silver.course';
        SET @start_time = SYSDATETIME();

        INSERT INTO silver.course (course_id, [subject], course_number, credits, course_title)
        SELECT course_id, [subject], course_number, credits, course_title
        FROM (
            SELECT
                TRIM(CAST(course_id AS NVARCHAR(50))) AS course_id,
                NULLIF(TRIM([subject]), '')           AS [subject],
                course_number,
                credits,
                NULLIF(TRIM(course_title), '')        AS course_title,
                ROW_NUMBER() OVER (
                    PARTITION BY TRIM(CAST(course_id AS NVARCHAR(50)))
                    ORDER BY (SELECT 1)
                ) AS rn
            FROM bronze.course
            WHERE course_id IS NOT NULL
        ) c
        WHERE rn = 1;

        SET @end_time = SYSDATETIME();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(20)) + ' sec';

        /* silver.programs */

        PRINT '------------------------------------------------';
        PRINT 'Loading silver.programs';
        SET @start_time = SYSDATETIME();

        INSERT INTO silver.programs (program_code, program_name, college)
        SELECT program_code, program_name, college
        FROM (
            SELECT
                TRIM(CAST(program_code AS NVARCHAR(50))) AS program_code,
                NULLIF(TRIM(program_name), '')           AS program_name,
                NULLIF(TRIM(college), '')                AS college,
                ROW_NUMBER() OVER (
                    PARTITION BY TRIM(CAST(program_code AS NVARCHAR(50)))
                    ORDER BY (SELECT 1)
                ) AS rn
            FROM bronze.programs
            WHERE program_code IS NOT NULL
        ) p
        WHERE rn = 1;

        SET @end_time = SYSDATETIME();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(20)) + ' sec';

        /* silver.program_learning_outcomes */

        PRINT '------------------------------------------------';
        PRINT 'Loading silver.program_learning_outcomes';
        SET @start_time = SYSDATETIME();

        INSERT INTO silver.program_learning_outcomes (plo_id, program_code, plo_description)
        SELECT plo_id, program_code, plo_description
        FROM (
            SELECT
                TRIM(CAST(plo_id AS NVARCHAR(50)))        AS plo_id,
                TRIM(CAST(program_code AS NVARCHAR(50)))  AS program_code,
                NULLIF(TRIM(plo_description), '')         AS plo_description,
                ROW_NUMBER() OVER (
                    PARTITION BY TRIM(CAST(plo_id AS NVARCHAR(50))),
                                 TRIM(CAST(program_code AS NVARCHAR(50)))
                    ORDER BY (SELECT 1)
                ) AS rn
            FROM bronze.program_learning_outcomes
            WHERE plo_id IS NOT NULL AND program_code IS NOT NULL
        ) plo
        WHERE rn = 1;

        SET @end_time = SYSDATETIME();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(20)) + ' sec';

        /* silver.students */

        PRINT '------------------------------------------------';
        PRINT 'Loading silver.students';
        SET @start_time = SYSDATETIME();

        INSERT INTO silver.students
        (
            student_id, first_name, last_name, dob, gender, race_ethnicity,
            first_gen_flag, pell_flag, residency, entry_cohort_term, hs_gpa,
            admit_type, email
        )
        SELECT
            student_id,
            first_name,
            last_name,
            dob,
            gender,
            race_ethnicity,
            first_gen_flag,
            pell_flag,
            residency,
            entry_cohort_term,
            hs_gpa,
            admit_type,
            email
        FROM (
            SELECT
                TRIM(CAST(student_id AS NVARCHAR(50))) AS student_id,
                NULLIF(TRIM(first_name), '')          AS first_name,
                NULLIF(TRIM(last_name), '')           AS last_name,
                dob,
                CASE WHEN gender LIKE 'M%' THEN 'Male'             
                     WHEN gender LIKE 'F%' THEN 'Female'
                     WHEN gender LIKE 'U%' THEN 'Undefined'
                     ELSE gender
                     END AS gender,
                NULLIF(TRIM(race_ethnicity), '')      AS race_ethnicity,
                CASE WHEN first_gen_flag IN (0,1) THEN first_gen_flag ELSE NULL END AS first_gen_flag,
                CASE WHEN pell_flag IN (0,1) THEN pell_flag ELSE NULL END           AS pell_flag,
                NULLIF(TRIM(residency), '')           AS residency,
                NULLIF(TRIM(entry_cohort_term), '')   AS entry_cohort_term,
                CASE WHEN hs_gpa BETWEEN 0 AND 4.500 THEN hs_gpa ELSE NULL END      AS hs_gpa,
                NULLIF(TRIM(admit_type), '')          AS admit_type,
                CONCAT(LOWER(first_name),LOWER(last_name),SUBSTRING(student_id,3,LEN(student_id)),'@student.univ.edu')     AS email,
                ROW_NUMBER() OVER (
                    PARTITION BY TRIM(CAST(student_id AS NVARCHAR(50)))
                    ORDER BY (SELECT 1)
                ) AS rn
            FROM bronze.students
            WHERE student_id IS NOT NULL
        ) s
        WHERE rn = 1;

        SET @end_time = SYSDATETIME();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(20)) + ' sec';

        /* ------------------------------------------------
           FACTS
        -------------------------------------------------*/

        /* silver.enrollments */

        PRINT '------------------------------------------------';
        PRINT 'Loading silver.enrollments';
        SET @start_time = SYSDATETIME();

        INSERT INTO silver.enrollments
        (
            student_id, term_code, program_code, enrollment_status,
            credits_attempted, credits_earned, gpa_term, academic_standing, advisor_staff_id
        )
        SELECT
            student_id, term_code, program_code, enrollment_status,
            credits_attempted, credits_earned, gpa_term, academic_standing, advisor_staff_id
        FROM (
            SELECT
                TRIM(CAST(student_id AS NVARCHAR(50))) AS student_id,
                CASE WHEN term_code='24FA20' THEN '2024FA'
                     WHEN term_code='2025Fall' THEN '2025FA'
                     WHEN term_code='25SP20'THEN '2025SP'
                     WHEN term_code='23FA20' THEN '2023FA'
                     WHEN term_code='24SP20' THEN '2024SP'
                     WHEN term_code='24SU20' THEN '2024SU'
                     WHEN term_code='2024Spring' THEN '2024SP'
                     WHEN term_code='2023Fall' THEN '2023FA'
                     WHEN term_code='2025Spring' THEN '2025SP'
                     WHEN term_code='25SU20' THEN '2025SU'
                     WHEN term_code='25FA20' THEN '2025FA'
                     WHEN term_code='2024Fall' THEN '2024FA'
                ELSE term_code 
                END AS term_code,
                NULLIF(TRIM(program_code), '')         AS program_code,
                NULLIF(TRIM(enrollment_status), '')    AS enrollment_status,
                credits_attempted,
                credits_earned,
                CASE WHEN gpa_term BETWEEN 0 AND 4.000 THEN gpa_term ELSE NULL END AS gpa_term,
                NULLIF(TRIM(academic_standing), '')    AS academic_standing,
                NULLIF(TRIM(advisor_staff_id), '')     AS advisor_staff_id,
                ROW_NUMBER() OVER (
                    PARTITION BY TRIM(CAST(student_id AS NVARCHAR(50))),
                                 TRIM(CAST(term_code AS NVARCHAR(50)))
                    ORDER BY (SELECT 1)
                ) AS rn
            FROM bronze.enrollments
            WHERE student_id IS NOT NULL AND term_code IS NOT NULL
        ) e
        WHERE rn = 1;

        SET @end_time = SYSDATETIME();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(20)) + ' sec';

        /* silver.advising_interactions */
  
        PRINT '------------------------------------------------';
        PRINT 'Loading silver.advising_interactions';
        SET @start_time = SYSDATETIME();

        INSERT INTO silver.advising_interactions
        (
            interaction_id, student_id, interaction_date, channel,
            reason, outcome, staff_id, follow_up_flag
        )
        SELECT
            interaction_id, student_id, interaction_date, channel,
            reason, outcome, staff_id, follow_up_flag
        FROM (
            SELECT
                TRIM(CAST(interaction_id AS NVARCHAR(50))) AS interaction_id,
                TRIM(CAST(student_id AS NVARCHAR(50)))     AS student_id,
                interaction_date,
                NULLIF(TRIM(channel), '')                  AS channel,
                NULLIF(TRIM(reason), '')                   AS reason,
                NULLIF(TRIM(outcome), '')                  AS outcome,
                NULLIF(TRIM(staff_id), '')                 AS staff_id,
                CASE WHEN follow_up_flag IN (0,1) THEN follow_up_flag ELSE NULL END AS follow_up_flag,
                ROW_NUMBER() OVER (
                    PARTITION BY TRIM(CAST(interaction_id AS NVARCHAR(50)))
                    ORDER BY interaction_date DESC
                ) AS rn
            FROM bronze.advising_interactions
            WHERE interaction_id IS NOT NULL AND student_id IS NOT NULL
        ) a
        WHERE rn = 1;

        SET @end_time = SYSDATETIME();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(20)) + ' sec';

        /* silver.assessment_results */
       
        PRINT '------------------------------------------------';
        PRINT 'Loading silver.assessment_results';
        SET @start_time = SYSDATETIME();

        INSERT INTO silver.assessment_results
        (
            assessment_id, student_id, term_code, program_code, plo_id,
            assessment_type, score_0to4, assessment_date
        )
        SELECT
            assessment_id, student_id, term_code, program_code, plo_id,
            assessment_type, score_0to4, assessment_date
        FROM (
            SELECT
                TRIM(CAST(assessment_id AS NVARCHAR(50))) AS assessment_id,
                TRIM(CAST(student_id AS NVARCHAR(50)))    AS student_id,
                CASE WHEN term_code='24FA20' THEN '2024FA'
                     WHEN term_code='2025Fall' THEN '2025FA'
                     WHEN term_code='25SP20'THEN '2025SP'
                     WHEN term_code='23FA20' THEN '2023FA'
                     WHEN term_code='24SP20' THEN '2024SP'
                     WHEN term_code='24SU20' THEN '2024SU'
                     WHEN term_code='2024Spring' THEN '2024SP'
                     WHEN term_code='2023Fall' THEN '2023FA'
                     WHEN term_code='2025Spring' THEN '2025SP'
                     WHEN term_code='25SU20' THEN '2025SU'
                     WHEN term_code='25FA20' THEN '2025FA'
                     WHEN term_code='2024Fall' THEN '2024FA'
                ELSE term_code 
                END AS term_code,
                NULLIF(TRIM(program_code), '')            AS program_code,
                NULLIF(TRIM(plo_id), '')                  AS plo_id,
                NULLIF(TRIM(assessment_type), '')         AS assessment_type,
                CASE WHEN score_0to4 BETWEEN 0 AND 4.00 THEN score_0to4 ELSE NULL END AS score_0to4,
                assessment_date,
                ROW_NUMBER() OVER (
                    PARTITION BY TRIM(CAST(assessment_id AS NVARCHAR(50)))
                    ORDER BY assessment_date DESC
                ) AS rn
            FROM bronze.assessment_results
            WHERE assessment_id IS NOT NULL AND student_id IS NOT NULL
        ) ar
        WHERE rn = 1;

        SET @end_time = SYSDATETIME();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(20)) + ' sec';

        /* silver.course_grades */
     
        PRINT '------------------------------------------------';
        PRINT 'Loading silver.course_grades';
        SET @start_time = SYSDATETIME();

        INSERT INTO silver.course_grades
        (
            student_id, term_code, course_id, section,
            grade, grade_point, attendance_rate, lms_logins, assignments_submitted_pct
        )
        SELECT
            student_id, term_code, course_id, section,
            grade, grade_point, attendance_rate, lms_logins, assignments_submitted_pct
        FROM (
            SELECT
                TRIM(CAST(student_id AS NVARCHAR(50))) AS student_id,
                CASE WHEN term_code='24FA20' THEN '2024FA'
                     WHEN term_code='2025Fall' THEN '2025FA'
                     WHEN term_code='25SP20'THEN '2025SP'
                     WHEN term_code='23FA20' THEN '2023FA'
                     WHEN term_code='24SP20' THEN '2024SP'
                     WHEN term_code='24SU20' THEN '2024SU'
                     WHEN term_code='2024Spring' THEN '2024SP'
                     WHEN term_code='2023Fall' THEN '2023FA'
                     WHEN term_code='2025Spring' THEN '2025SP'
                     WHEN term_code='25SU20' THEN '2025SU'
                     WHEN term_code='25FA20' THEN '2025FA'
                     WHEN term_code='2024Fall' THEN '2024FA'
                ELSE term_code 
                END AS term_code,
                TRIM(CAST(course_id AS NVARCHAR(50)))  AS course_id,
                TRIM(CAST(section AS NVARCHAR(50)))    AS section,
                NULLIF(TRIM(grade), '')                AS grade,
                CASE WHEN grade_point BETWEEN 0 AND 4.000 THEN grade_point ELSE NULL END AS grade_point,
                CASE WHEN attendance_rate BETWEEN 0 AND 1 THEN attendance_rate ELSE NULL END AS attendance_rate,
                CASE WHEN lms_logins >= 0 THEN lms_logins ELSE NULL END AS lms_logins,
                CASE WHEN assignments_submitted_pct BETWEEN 0 AND 1 THEN assignments_submitted_pct ELSE NULL END AS assignments_submitted_pct,
                ROW_NUMBER() OVER (
                    PARTITION BY TRIM(CAST(student_id AS NVARCHAR(50))),
                                 TRIM(CAST(term_code AS NVARCHAR(50))),
                                 TRIM(CAST(course_id AS NVARCHAR(50))),
                                 TRIM(CAST(section AS NVARCHAR(50)))
                    ORDER BY (SELECT 1)
                ) AS rn
            FROM bronze.course_grades
            WHERE student_id IS NOT NULL AND term_code IS NOT NULL AND course_id IS NOT NULL AND section IS NOT NULL
        ) cg
        WHERE rn = 1;

        SET @end_time = SYSDATETIME();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(20)) + ' sec';

        /* silver.survey */

        PRINT '------------------------------------------------';
        PRINT 'Loading silver.survey';
        SET @start_time = SYSDATETIME();

        INSERT INTO silver.survey
        (
            response_id, student_id, survey_type, response_date,
            sense_belonging_1to5, academic_confidence_1to5, stress_1to5,
            resources_awareness_1to5, advisor_satisfaction_1to5
        )
        SELECT
            response_id, student_id, survey_type, response_date,
            sense_belonging_1to5, academic_confidence_1to5, stress_1to5,
            resources_awareness_1to5, advisor_satisfaction_1to5
        FROM (
            SELECT
                TRIM(CAST(response_id AS NVARCHAR(50))) AS response_id,
                TRIM(CAST(student_id AS NVARCHAR(50)))  AS student_id,
                NULLIF(TRIM(survey_type), '')           AS survey_type,
                response_date,
                CASE WHEN sense_belonging_1to5 BETWEEN 1 AND 5 THEN sense_belonging_1to5 ELSE NULL END AS sense_belonging_1to5,
                CASE WHEN academic_confidence_1to5 BETWEEN 1 AND 5 THEN academic_confidence_1to5 ELSE NULL END AS academic_confidence_1to5,
                CASE WHEN stress_1to5 BETWEEN 1 AND 5 THEN stress_1to5 ELSE NULL END AS stress_1to5,
                CASE WHEN resources_awareness_1to5 BETWEEN 1 AND 5 THEN resources_awareness_1to5 ELSE NULL END AS resources_awareness_1to5,
                CASE WHEN advisor_satisfaction_1to5 BETWEEN 1 AND 5 THEN advisor_satisfaction_1to5 ELSE NULL END AS advisor_satisfaction_1to5,
                ROW_NUMBER() OVER (
                    PARTITION BY TRIM(CAST(response_id AS NVARCHAR(50)))
                    ORDER BY response_date DESC
                ) AS rn
            FROM bronze.survey
            WHERE response_id IS NOT NULL AND student_id IS NOT NULL
        ) sv
        WHERE rn = 1;

        SET @end_time = SYSDATETIME();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(20)) + ' sec';

        SET @batch_end_time = SYSDATETIME();
        PRINT '================================================';
        PRINT 'Silver load complete';
        PRINT 'Batch end:   ' + CONVERT(VARCHAR(30), @batch_end_time, 121);
        PRINT 'Total secs:  ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR(20));
        PRINT '================================================';
    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        PRINT 'Message:   ' + ERROR_MESSAGE();
        PRINT 'Number:    ' + CAST(ERROR_NUMBER() AS VARCHAR(20));
        PRINT 'State:     ' + CAST(ERROR_STATE() AS VARCHAR(20));
        PRINT 'Severity:  ' + CAST(ERROR_SEVERITY() AS VARCHAR(20));
        PRINT 'Line:      ' + CAST(ERROR_LINE() AS VARCHAR(20));
        PRINT 'Procedure: ' + ISNULL(ERROR_PROCEDURE(), '(adhoc)');
        PRINT '================================================';
        THROW;
    END CATCH
END;
GO
