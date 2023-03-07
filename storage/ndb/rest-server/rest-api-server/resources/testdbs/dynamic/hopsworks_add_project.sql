-- Sed keywords PROJECT_NAME, PROJECT_NUMBER

INSERT INTO
    `project`
VALUES
    (
        PROJECT_NUMBER,
        322,
        'PROJECT_NAME',
        322,
        'PROJECT_NAME',
        'macho@hopsworks.ai',
        '2022-05-30 14:17:22',
        '2032-05-30',
        0,
        0,
        NULL,
        'A demo project for getting started with featurestore',
        'NOLIMIT',
        '2022-05-30 14:17:38',
        100,
        'demo_fs_meb10000:1653921933268-2.6.0-SNAPSHOT.1',
        1
    );

INSERT INTO
    `project_team`
VALUES
    (
        PROJECT_NUMBER,
        'macho@hopsworks.ai',
        'Data scientist',
        '2022-06-01 13:28:05'
    );