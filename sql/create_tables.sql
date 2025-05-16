CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create the user table
create table if not exists "user" (
    id VARCHAR(255) not null PRIMARY KEY, -- Unique identifier for the user
    email VARCHAR(255) NOT NULL UNIQUE, -- User's email for identification
    first_name VARCHAR(64), -- User's first name for display
    last_name VARCHAR(64), -- User's last name for display
    dept VARCHAR(255), -- Department for analysis (e.g., grouping by department)
    role_name VARCHAR(255), -- Role for access control and analysis
    created_at TIMESTAMP WITH TIME ZONE, -- When the user was created for tracking
    last_login_time TIMESTAMP WITH TIME ZONE, -- Last login for activity tracking
	group_names TEXT[] DEFAULT '{}' -- Groups in Zoom
);

-- Create the meeting table
CREATE TABLE meeting (
    id BIGINT, -- Unique meeting ID (meeting number)
    uuid VARCHAR(255) NOT NULL PRIMARY KEY, -- UUID for unique meeting instance
    host_id VARCHAR(255) NOT NULL, -- Links to user table
    topic VARCHAR(255), -- Meeting topic for reference
    created_at TIMESTAMP WITH TIME ZONE, -- Created time for analysis
    start_time TIMESTAMP WITH TIME ZONE, -- Start time for analysis
    end_time TIMESTAMP WITH TIME ZONE, -- End time for duration calculation
    duration INTEGER, -- Duration in minutes for analysis
    participants_count INTEGER, -- Number of participants for analysis
    type INTEGER, -- Meeting type for filtering (e.g., scheduled, instant)
    FOREIGN KEY (host_id) REFERENCES "user"(id)
);

-- Create the participant table
CREATE TABLE participant (
    index UUID PRIMARY KEY default uuid_generate_v4(), -- Auto-incrementing ID for participant records
    id VARCHAR(255), -- Links to user table (nullable for non-logged-in users)
    meeting_uuid VARCHAR(255) NOT NULL, -- Links to meeting table
    user_id VARCHAR(255), -- Links to user table (nullable for non-logged-in users)
    name VARCHAR(255), -- Display name for reference
    user_email VARCHAR(255), -- Email for identification (nullable for non-logged-in users)
    join_time TIMESTAMP WITH TIME ZONE, -- Join time for attendance tracking
    leave_time TIMESTAMP WITH TIME ZONE, -- Leave time for duration calculation
    duration INTEGER, -- Duration in seconds for analysis
    internal_user BOOLEAN DEFAULT FALSE, -- Flag for internal vs external users
    FOREIGN KEY (meeting_uuid) REFERENCES meeting(uuid),
    FOREIGN KEY (id) REFERENCES "user"(id)
);

-- Create the recording table
CREATE TABLE recording (
    id VARCHAR(255) not null PRIMARY KEY, -- Unique file ID from Zoom
    meeting_uuid VARCHAR(255) NOT NULL, -- Links to meeting table
    file_type VARCHAR(50), -- Type of recording (e.g., MP4, M4A)
    file_size BIGINT, -- File size in bytes for storage analysis
	file_extension VARCHAR(10), -- File extension for downloading
    recording_start TIMESTAMP WITH TIME ZONE, -- Start time of recording
    recording_end TIMESTAMP WITH TIME ZONE, -- End time of recording
    recording_type VARCHAR(255), -- Specific type (e.g., shared_screen)
    download_url TEXT, -- URL to download the file
    -- file_content BYTE, -- Blob storage for the actual recording file
	file_path TEXT, -- Store S3 path to the recording
    FOREIGN KEY (meeting_uuid) REFERENCES meeting(uuid)
);

-- Add indexes for performance
CREATE INDEX idx_meeting_host_id ON meeting(host_id);
CREATE INDEX idx_participant_meeting_id ON participant(meeting_uuid);
CREATE INDEX idx_participant_user_id ON participant(user_id);
CREATE INDEX idx_recording_meeting_id ON recording(meeting_uuid);