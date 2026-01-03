/*
===========================================================================================================
DDL Script: Create Bronze Tables (Full Script)
===========================================================================================================
Purpose:
  - Ensures bronze schema exists
  - Drops and recreates bronze tables
===========================================================================================================
*/
USE College_DataWarehouse;

-- Ensure schema exists
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO

/*==========================================================
  DROP TABLES If They exist
==========================================================*/
IF OBJECT_ID('bronze.advising_interactions','U') IS NOT NULL DROP TABLE bronze.advising_interactions;
IF OBJECT_ID('bronze.assessment_results','U')      IS NOT NULL DROP TABLE bronze.assessment_results;
IF OBJECT_ID('bronze.course_grades','U')           IS NOT NULL DROP TABLE bronze.course_grades;
IF OBJECT_ID('bronze.course','U')                  IS NOT NULL DROP TABLE bronze.course;
IF OBJECT_ID('bronze.enrollments','U')             IS NOT NULL DROP TABLE bronze.enrollments;
IF OBJECT_ID('bronze.program_learning_outcomes','U') IS NOT NULL DROP TABLE bronze.program_learning_outcomes;
IF OBJECT_ID('bronze.programs','U')                IS NOT NULL DROP TABLE bronze.programs;
IF OBJECT_ID('bronze.staff','U')                   IS NOT NULL DROP TABLE bronze.staff;
IF OBJECT_ID('bronze.students','U')                IS NOT NULL DROP TABLE bronze.students;
IF OBJECT_ID('bronze.survey','U')                  IS NOT NULL DROP TABLE bronze.survey;
IF OBJECT_ID('bronze.terms','U')                   IS NOT NULL DROP TABLE bronze.terms;
GO

/*==========================================================
  CREATE TABLES
==========================================================*/

CREATE TABLE bronze.staff(
    staff_id   CHAR(6)        NOT NULL,
    staff_name NVARCHAR(100)  NULL,
    unit       NVARCHAR(50)   NULL,
    email      NVARCHAR(100)  NULL
);
GO

CREATE TABLE bronze.terms(
    term_code    CHAR(6)       NOT NULL,
    term_name    NVARCHAR(20)  NULL,
    [start_date] DATE          NULL,
    [end_date]   DATE          NULL
);
GO

CREATE TABLE bronze.course(
    course_id      VARCHAR(20)  NOT NULL,
    [subject]      NVARCHAR(10) NULL,
    course_number  INT          NULL,
    credits        INT          NULL,
    course_title   NVARCHAR(100) NULL
);
GO


CREATE TABLE bronze.programs(
    program_code  VARCHAR(20)  NOT NULL,
    [program_name]  NVARCHAR(50) NULL,
    college       NVARCHAR(50) NULL
);
GO

CREATE TABLE bronze.program_learning_outcomes(
    plo_id          CHAR(5)        NOT NULL,
    program_code    VARCHAR(20)    NOT NULL,
    plo_description NVARCHAR(200)  NULL
);
GO

CREATE TABLE bronze.students(
    student_id        CHAR(7)        NOT NULL,
    first_name        NVARCHAR(50)   NULL,
    last_name         NVARCHAR(50)   NULL,
    dob               DATE           NULL,
    gender            NVARCHAR(20)   NULL,
    race_ethnicity    NVARCHAR(50)   NULL,
    first_gen_flag    INT            NULL CHECK (first_gen_flag IN (0,1)),
    pell_flag         INT            NULL CHECK (pell_flag IN (0,1)),
    residency         NVARCHAR(50)   NULL,
    entry_cohort_term CHAR(6)        NULL,
    hs_gpa            DECIMAL(4,3)   NULL,
    admit_type        NVARCHAR(30)   NULL,
    email             NVARCHAR(100)  NULL
);
GO

CREATE TABLE bronze.enrollments (
    student_id         CHAR(7)       NOT NULL,
    term_code          VARCHAR(20)       NOT NULL,
    program_code       VARCHAR(20)   NULL,
    enrollment_status  NVARCHAR(30)  NULL,
    credits_attempted  INT           NULL,
    credits_earned     INT           NULL,
    gpa_term           DECIMAL(4,3)  NULL,
    academic_standing  NVARCHAR(30)  NULL,
    advisor_staff_id   CHAR(6)       NULL
    );
GO

CREATE TABLE bronze.advising_interactions(
    interaction_id    CHAR(9)       NOT NULL,
    student_id        CHAR(7)       NOT NULL,
    interaction_date  DATE          NULL,
    channel           NVARCHAR(50)  NULL,
    reason            NVARCHAR(50)  NULL,
    outcome           NVARCHAR(50)  NULL,
    staff_id          CHAR(6)       NULL,
    follow_up_flag    INT           NULL CHECK (follow_up_flag IN (0,1))
);
GO

CREATE TABLE bronze.assessment_results(
    assessment_id    CHAR(8)       NOT NULL,
    student_id       CHAR(7)       NOT NULL,
    term_code        CHAR(8)       NULL,
    program_code     VARCHAR(20)   NULL,
    plo_id           CHAR(5)       NULL,
    assessment_type  NVARCHAR(20)  NULL,
    score_0to4       DECIMAL(3,2)  NULL,
    assessment_date  DATE          NULL
);
GO

CREATE TABLE bronze.course_grades(
    student_id                CHAR(7)       NOT NULL,
    term_code                 VARCHAR(20)       NOT NULL,
    course_id                 VARCHAR(20)   NOT NULL,
    section                   VARCHAR(10)   NOT NULL,
    grade                     NVARCHAR(10)  NULL,
    grade_point               DECIMAL(4,3)  NULL,
    attendance_rate           DECIMAL(5,4)  NULL,  -- 0.0000 to 1.0000
    lms_logins                INT           NULL,
    assignments_submitted_pct DECIMAL(5,4)  NULL,  -- 0.0000 to 1.0000
);
GO

CREATE TABLE bronze.survey(
    response_id               CHAR(8)       NOT NULL,
    student_id                CHAR(7)       NOT NULL,
    survey_type               NVARCHAR(30)  NULL,
    response_date             DATE          NULL,
    sense_belonging_1to5      INT           NULL CHECK (sense_belonging_1to5 BETWEEN 1 AND 5),
    academic_confidence_1to5  INT           NULL CHECK (academic_confidence_1to5 BETWEEN 1 AND 5),
    stress_1to5               INT           NULL CHECK (stress_1to5 BETWEEN 1 AND 5),
    resources_awareness_1to5  INT           NULL CHECK (resources_awareness_1to5 BETWEEN 1 AND 5),
    advisor_satisfaction_1to5 INT           NULL CHECK (advisor_satisfaction_1to5 BETWEEN 1 AND 5)
);
GO
