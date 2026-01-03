/*
===========================================================================================================
DDL Script: Create silver Tables 
===========================================================================================================
Purpose:
  - Ensures silver schema exists
  - Drops and recreates silver tables
  - Adds basic PKs + FK + sanity checks suitable for silver
===========================================================================================================
*/
USE College_DataWarehouse;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
GO

-- Drop (facts first)
IF OBJECT_ID('silver.course_grades','U') IS NOT NULL DROP TABLE silver.course_grades;
IF OBJECT_ID('silver.assessment_results','U') IS NOT NULL DROP TABLE silver.assessment_results;
IF OBJECT_ID('silver.advising_interactions','U') IS NOT NULL DROP TABLE silver.advising_interactions;
IF OBJECT_ID('silver.enrollments','U') IS NOT NULL DROP TABLE silver.enrollments;
IF OBJECT_ID('silver.survey','U') IS NOT NULL DROP TABLE silver.survey;

IF OBJECT_ID('silver.program_learning_outcomes','U') IS NOT NULL DROP TABLE silver.program_learning_outcomes;
IF OBJECT_ID('silver.course','U') IS NOT NULL DROP TABLE silver.course;
IF OBJECT_ID('silver.programs','U') IS NOT NULL DROP TABLE silver.programs;
IF OBJECT_ID('silver.students','U') IS NOT NULL DROP TABLE silver.students;
IF OBJECT_ID('silver.staff','U') IS NOT NULL DROP TABLE silver.staff;
IF OBJECT_ID('silver.terms','U') IS NOT NULL DROP TABLE silver.terms;
GO

CREATE TABLE silver.staff(
    staff_id   VARCHAR(6)       NOT NULL,
    first_name NVARCHAR(100)    NULL,
    last_name NVARCHAR(100) NULL,
    unit       NVARCHAR(50)     NULL,
    email      NVARCHAR(254)    NULL,
    created_at DATETIME2(3)     NOT NULL CONSTRAINT DF_silver_staff_created_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_silver_staff PRIMARY KEY (staff_id)
);
GO

CREATE TABLE silver.terms(
    term_code  VARCHAR(10)      NOT NULL,  -- supports 2023Fall, or normalized codes
    term_name  NVARCHAR(20)     NULL,
    start_date DATE             NULL,
    end_date   DATE             NULL,
    created_at DATETIME2(3)     NOT NULL CONSTRAINT DF_silver_terms_created_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_silver_terms PRIMARY KEY (term_code),
    CONSTRAINT CK_silver_terms_dates CHECK (end_date IS NULL OR start_date IS NULL OR end_date >= start_date)
);
GO

CREATE TABLE silver.course(
    course_id     VARCHAR(20)    NOT NULL,
    subject       NVARCHAR(10)   NULL,
    course_number INT            NULL,
    credits       DECIMAL(4,2)   NULL,
    course_title  NVARCHAR(100)  NULL,
    created_at    DATETIME2(3)   NOT NULL CONSTRAINT DF_silver_course_created_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_silver_course PRIMARY KEY (course_id)
);
GO

CREATE TABLE silver.programs(
    program_code  VARCHAR(20)    NOT NULL,
    program_name  NVARCHAR(50)   NULL,
    college       NVARCHAR(50)   NULL,
    created_at    DATETIME2(3)   NOT NULL CONSTRAINT DF_silver_programs_created_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_silver_programs PRIMARY KEY (program_code)
);
GO

CREATE TABLE silver.program_learning_outcomes(
    plo_id          VARCHAR(5)      NOT NULL,
    program_code    VARCHAR(20)     NOT NULL,
    plo_description NVARCHAR(200)   NULL,
    created_at      DATETIME2(3)    NOT NULL CONSTRAINT DF_silver_plos_created_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_silver_plos PRIMARY KEY (plo_id, program_code)
);
GO

CREATE TABLE silver.students(
    student_id        VARCHAR(7)      NOT NULL,
    first_name        NVARCHAR(50)    NULL,
    last_name         NVARCHAR(50)    NULL,
    dob               DATE            NULL,
    gender            NVARCHAR(20)    NULL,
    race_ethnicity    NVARCHAR(50)    NULL,
    first_gen_flag    BIT             NULL,
    pell_flag         BIT             NULL,
    residency         NVARCHAR(50)    NULL,
    entry_cohort_term VARCHAR(10)     NULL,
    hs_gpa            DECIMAL(4,3)    NULL,
    admit_type        NVARCHAR(30)    NULL,
    email             NVARCHAR(254)   NULL,
    created_at        DATETIME2(3)    NOT NULL CONSTRAINT DF_silver_students_created_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_silver_students PRIMARY KEY (student_id),
    CONSTRAINT CK_silver_students_hs_gpa CHECK (hs_gpa IS NULL OR (hs_gpa BETWEEN 0 AND 4.500))
);
GO

CREATE TABLE silver.enrollments(
    student_id        VARCHAR(7)     NOT NULL,
    term_code         VARCHAR(10)    NOT NULL,
    program_code      VARCHAR(20)    NULL,
    enrollment_status NVARCHAR(30)   NULL,
    credits_attempted INT            NULL,
    credits_earned    INT            NULL,
    gpa_term          DECIMAL(4,3)   NULL,
    academic_standing NVARCHAR(30)   NULL,
    advisor_staff_id  VARCHAR(6)     NULL,
    created_at        DATETIME2(3)   NOT NULL CONSTRAINT DF_silver_enrollments_created_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_silver_enrollments PRIMARY KEY (student_id, term_code),
    CONSTRAINT CK_silver_enrollments_gpa CHECK (gpa_term IS NULL OR (gpa_term BETWEEN 0 AND 4.000))
);
GO

CREATE TABLE silver.advising_interactions(
    interaction_id   VARCHAR(9)      NOT NULL,
    student_id       VARCHAR(7)      NOT NULL,
    interaction_date DATE            NULL,
    channel          NVARCHAR(50)    NULL,
    reason           NVARCHAR(50)    NULL,
    outcome          NVARCHAR(50)    NULL,
    staff_id         VARCHAR(6)      NULL,
    follow_up_flag   BIT             NULL,
    created_at       DATETIME2(3)    NOT NULL CONSTRAINT DF_silver_advising_created_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_silver_advising PRIMARY KEY (interaction_id)
);
GO

CREATE TABLE silver.assessment_results(
    assessment_id    VARCHAR(8)      NOT NULL,
    student_id       VARCHAR(7)      NOT NULL,
    term_code        VARCHAR(10)     NULL,
    program_code     VARCHAR(20)     NULL,
    plo_id           VARCHAR(5)      NULL,
    assessment_type  NVARCHAR(20)    NULL,
    score_0to4       DECIMAL(3,2)    NULL,
    assessment_date  DATE            NULL,
    created_at       DATETIME2(3)    NOT NULL CONSTRAINT DF_silver_assessment_created_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_silver_assessment PRIMARY KEY (assessment_id),
    CONSTRAINT CK_silver_assessment_score CHECK (score_0to4 IS NULL OR (score_0to4 BETWEEN 0 AND 4.00))
);
GO

CREATE TABLE silver.course_grades(
    student_id                VARCHAR(7)     NOT NULL,
    term_code                 VARCHAR(10)    NOT NULL,
    course_id                 VARCHAR(20)    NOT NULL,
    section                   VARCHAR(10)    NOT NULL,
    grade                     NVARCHAR(10)   NULL,
    grade_point               DECIMAL(4,3)   NULL,
    attendance_rate           DECIMAL(5,4)   NULL,
    lms_logins                INT            NULL,
    assignments_submitted_pct DECIMAL(5,4)   NULL,
    created_at                DATETIME2(3)   NOT NULL CONSTRAINT DF_silver_course_grades_created_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_silver_course_grades PRIMARY KEY (student_id, term_code, course_id, section),
    CONSTRAINT CK_silver_course_grades_attendance CHECK (attendance_rate IS NULL OR attendance_rate BETWEEN 0 AND 1),
    CONSTRAINT CK_silver_course_grades_assignments CHECK (assignments_submitted_pct IS NULL OR assignments_submitted_pct BETWEEN 0 AND 1),
    CONSTRAINT CK_silver_course_grades_lms CHECK (lms_logins IS NULL OR lms_logins >= 0)
);
GO

CREATE TABLE silver.survey(
    response_id               VARCHAR(8)      NOT NULL,
    student_id                VARCHAR(7)      NOT NULL,
    survey_type               NVARCHAR(30)    NULL,
    response_date             DATE            NULL,
    sense_belonging_1to5      INT             NULL CHECK (sense_belonging_1to5 BETWEEN 1 AND 5),
    academic_confidence_1to5  INT             NULL CHECK (academic_confidence_1to5 BETWEEN 1 AND 5),
    stress_1to5               INT             NULL CHECK (stress_1to5 BETWEEN 1 AND 5),
    resources_awareness_1to5  INT             NULL CHECK (resources_awareness_1to5 BETWEEN 1 AND 5),
    advisor_satisfaction_1to5 INT             NULL CHECK (advisor_satisfaction_1to5 BETWEEN 1 AND 5),
    created_at                DATETIME2(3)    NOT NULL CONSTRAINT DF_silver_survey_created_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_silver_survey PRIMARY KEY (response_id)
);
GO
