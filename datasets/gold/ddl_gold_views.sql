/*
===============================================================================
DDL Script: Create Gold Views (Higher Education Star Schema)
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse.
    The Gold layer represents the final dimension and fact views (Star Schema).

    Each view performs transformations and combines data from the Silver layer
    to produce a clean, enriched, and analytics-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Ensure Schema Exists: gold
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO


/*##############################################################################
  DIMENSIONS
##############################################################################*/

-- =============================================================================
-- Create Dimension: gold.dim_students
-- =============================================================================
IF OBJECT_ID('gold.dim_students', 'V') IS NOT NULL
    DROP VIEW gold.dim_students;
GO

CREATE VIEW gold.dim_students AS
SELECT
    ROW_NUMBER() OVER (ORDER BY s.student_id) AS student_key,   -- Surrogate key
    s.student_id,
    s.first_name,
    s.last_name,
    CONCAT(s.first_name, ' ', s.last_name) AS full_name,
    s.dob,
    s.gender,
    s.race_ethnicity,
    s.first_gen_flag,
    s.pell_flag,
    s.residency,
    s.entry_cohort_term,
    s.hs_gpa,
    s.admit_type,
    s.email
FROM silver.students s;
GO


-- =============================================================================
-- Create Dimension: gold.dim_staff
-- =============================================================================
IF OBJECT_ID('gold.dim_staff', 'V') IS NOT NULL
    DROP VIEW gold.dim_staff;
GO

CREATE VIEW gold.dim_staff AS
SELECT
    ROW_NUMBER() OVER (ORDER BY st.staff_id) AS staff_key,      -- Surrogate key
    st.staff_id,
    st.first_name,
    st.last_name,
    CONCAT(st.first_name, ' ', st.last_name) AS full_name,
    st.unit,
    st.email
FROM silver.staff st;
GO


-- =============================================================================
-- Create Dimension: gold.dim_terms
-- =============================================================================
IF OBJECT_ID('gold.dim_terms', 'V') IS NOT NULL
    DROP VIEW gold.dim_terms;
GO

CREATE VIEW gold.dim_terms AS
SELECT
    ROW_NUMBER() OVER (ORDER BY t.term_code) AS term_key,       -- Surrogate key
    t.term_code,
    t.term_name,
    t.start_date,
    t.end_date,
    TRY_CONVERT(INT, LEFT(t.term_code, 4)) AS term_year,
    CASE RIGHT(t.term_code, 2)
        WHEN 'SP' THEN 'Spring'
        WHEN 'SU' THEN 'Summer'
        WHEN 'FA' THEN 'Fall'
        WHEN 'WI' THEN 'Winter'
        ELSE 'Other'
    END AS term_season
FROM silver.terms t;
GO


-- =============================================================================
-- Create Dimension: gold.dim_courses
-- =============================================================================
IF OBJECT_ID('gold.dim_courses', 'V') IS NOT NULL
    DROP VIEW gold.dim_courses;
GO

CREATE VIEW gold.dim_courses AS
SELECT
    ROW_NUMBER() OVER (ORDER BY c.course_id) AS course_key,     -- Surrogate key
    c.course_id,
    c.[subject],
    c.course_number,
    c.credits,
    c.course_title,
    CONCAT(c.[subject], ' ', c.course_number) AS course_code
FROM silver.course c;
GO


-- =============================================================================
-- Create Dimension: gold.dim_programs
-- =============================================================================
IF OBJECT_ID('gold.dim_programs', 'V') IS NOT NULL
    DROP VIEW gold.dim_programs;
GO

CREATE VIEW gold.dim_programs AS
SELECT
    ROW_NUMBER() OVER (ORDER BY p.program_code) AS program_key, -- Surrogate key
    p.program_code,
    p.program_name,
    p.college
FROM silver.programs p;
GO


-- =============================================================================
-- Create Dimension: gold.dim_program_learning_outcomes
-- =============================================================================
IF OBJECT_ID('gold.dim_program_learning_outcomes', 'V') IS NOT NULL
    DROP VIEW gold.dim_program_learning_outcomes;
GO

CREATE VIEW gold.dim_program_learning_outcomes AS
SELECT
    ROW_NUMBER() OVER (ORDER BY plo.program_code, plo.plo_id) AS plo_key, -- Surrogate key
    plo.plo_id,
    plo.program_code,
    dp.program_key,
    dp.program_name,
    plo.plo_description
FROM silver.program_learning_outcomes plo
LEFT JOIN gold.dim_programs dp
    ON plo.program_code = dp.program_code;
GO


/*##############################################################################
  FACTS
##############################################################################*/

-- =============================================================================
-- Create Fact: gold.fact_enrollments
-- =============================================================================
IF OBJECT_ID('gold.fact_enrollments', 'V') IS NOT NULL
    DROP VIEW gold.fact_enrollments;
GO

CREATE VIEW gold.fact_enrollments AS
SELECT
    ds.student_key,
    dt.term_key,
    dp.program_key,
    dsa.staff_key AS advisor_staff_key,

    e.student_id,
    e.term_code,
    e.program_code,
    e.enrollment_status,
    e.credits_attempted,
    e.credits_earned,
    e.gpa_term,
    e.academic_standing,
    e.advisor_staff_id
FROM silver.enrollments e
LEFT JOIN gold.dim_students ds
    ON e.student_id = ds.student_id
LEFT JOIN gold.dim_terms dt
    ON e.term_code = dt.term_code
LEFT JOIN gold.dim_programs dp
    ON e.program_code = dp.program_code
LEFT JOIN gold.dim_staff dsa
    ON e.advisor_staff_id = dsa.staff_id;
GO


-- =============================================================================
-- Create Fact: gold.fact_course_grades
-- =============================================================================
IF OBJECT_ID('gold.fact_course_grades', 'V') IS NOT NULL
    DROP VIEW gold.fact_course_grades;
GO

CREATE VIEW gold.fact_course_grades AS
SELECT
    ds.student_key,
    dt.term_key,
    dc.course_key,
    dp.program_key,

    cg.student_id,
    cg.term_code,
    cg.course_id,
    cg.section,
    cg.grade,
    cg.grade_point,
    cg.attendance_rate,
    cg.lms_logins,
    cg.assignments_submitted_pct
FROM silver.course_grades cg
LEFT JOIN gold.dim_students ds
    ON cg.student_id = ds.student_id
LEFT JOIN gold.dim_terms dt
    ON cg.term_code = dt.term_code
LEFT JOIN gold.dim_courses dc
    ON cg.course_id = dc.course_id
LEFT JOIN silver.enrollments e
    ON cg.student_id = e.student_id AND cg.term_code = e.term_code
LEFT JOIN gold.dim_programs dp
    ON e.program_code = dp.program_code;
GO


-- =============================================================================
-- Create Fact: gold.fact_assessment_results
-- =============================================================================
IF OBJECT_ID('gold.fact_assessment_results', 'V') IS NOT NULL
    DROP VIEW gold.fact_assessment_results;
GO

CREATE VIEW gold.fact_assessment_results AS
SELECT
    ds.student_key,
    dt.term_key,
    dp.program_key,
    dplo.plo_key,

    ar.assessment_id,
    ar.student_id,
    ar.term_code,
    ar.program_code,
    ar.plo_id,
    ar.assessment_type,
    ar.score_0to4,
    ar.assessment_date
FROM silver.assessment_results ar
LEFT JOIN gold.dim_students ds
    ON ar.student_id = ds.student_id
LEFT JOIN gold.dim_terms dt
    ON ar.term_code = dt.term_code
LEFT JOIN gold.dim_programs dp
    ON ar.program_code = dp.program_code
LEFT JOIN gold.dim_program_learning_outcomes dplo
    ON ar.program_code = dplo.program_code
   AND ar.plo_id = dplo.plo_id;
GO


-- =============================================================================
-- Create Fact: gold.fact_advising_interactions
-- =============================================================================
IF OBJECT_ID('gold.fact_advising_interactions', 'V') IS NOT NULL
    DROP VIEW gold.fact_advising_interactions;
GO

CREATE VIEW gold.fact_advising_interactions AS
SELECT
    ds.student_key,
    dstaff.staff_key,
    dterm.term_key,

    ai.interaction_id,
    ai.student_id,
    ai.interaction_date,
    ai.channel,
    ai.reason,
    ai.outcome,
    ai.staff_id,
    ai.follow_up_flag
FROM silver.advising_interactions ai
LEFT JOIN gold.dim_students ds
    ON ai.student_id = ds.student_id
LEFT JOIN gold.dim_staff dstaff
    ON ai.staff_id = dstaff.staff_id
OUTER APPLY (
    SELECT TOP 1 dt.term_key
    FROM gold.dim_terms dt
    WHERE ai.interaction_date >= dt.start_date
      AND ai.interaction_date <= dt.end_date
    ORDER BY dt.start_date DESC
) dterm;
GO


-- =============================================================================
-- Create Fact: gold.fact_survey_responses
-- =============================================================================
IF OBJECT_ID('gold.fact_survey_responses', 'V') IS NOT NULL
    DROP VIEW gold.fact_survey_responses;
GO

CREATE VIEW gold.fact_survey_responses AS
SELECT
    ds.student_key,
    dterm.term_key,
    dp.program_key,

    sv.response_id,
    sv.student_id,
    sv.survey_type,
    sv.response_date,
    sv.sense_belonging_1to5,
    sv.academic_confidence_1to5,
    sv.stress_1to5,
    sv.resources_awareness_1to5,
    sv.advisor_satisfaction_1to5
FROM silver.survey sv
LEFT JOIN gold.dim_students ds
    ON sv.student_id = ds.student_id
OUTER APPLY (
    SELECT TOP 1 dt.term_key, dt.term_code
    FROM gold.dim_terms dt
    WHERE sv.response_date >= dt.start_date
      AND sv.response_date <= dt.end_date
    ORDER BY dt.start_date DESC
) dterm
LEFT JOIN silver.enrollments e
    ON sv.student_id = e.student_id
   AND e.term_code = dterm.term_code
LEFT JOIN gold.dim_programs dp
    ON e.program_code = dp.program_code;
GO
