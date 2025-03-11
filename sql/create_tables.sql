CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

create table if not exists "user" (
	id text not null primary key, -- Zoom user ID
	first_name text,
	last_name text,
	email text,
	"type" smallint,
	zoom_workplace smallint,
	on_prem bool,
	role_name text,
	use_pmi bool,
	personal_meeting_url text,
	timezone text,
	verified bool,
	dept text,
	last_login_time timestamp,
	last_client_version text,
	pic_url text,
	cms_user_id text,
	jid text,
	group_ids text,
	group_names text[],
	im_group_ids text,
	account_id text,
	"language" text,
	phone_country text,
	phone_number text,
	"status" varchar(8),
	job_title text,
	"location" text,
	login_types smallint[],
	"cluster" varchar(4),
	user_created_at timestamp,
	company text,
	vanity_url text,
	manager text,
	isActive bool
);

create table if not exists meeting (
	instance_id text, -- Meeting instance id
	id bigint,
	host_id text,
	host_email text,
	assistant_id text,
	topic text,
	"type" smallint,
	"status" varchar(7),
	scheduled_start_time timestamp,
	scheduled_duration smallint,
	timezone text,
	created_at timestamp,
	join_url text,
	"password" text,
	auto_recording text,
	recording_count int,
	"uuid" text not null primary key, -- Meeting id
	start_time timestamp,
	end_time timestamp,
	duration smallint,
	participants_count smallint,
	dept text,
	"source" text,
	constraint fk_user
		foreign Key(host_id)
			references "user"(id)
);

create table if not exists participant (
	key_id uuid primary key default uuid_generate_v4(),
	meeting_id text,
	id bigint, --numeric
	user_id text,
	join_time timestamp,
	leave_time timestamp,
	duration smallint,
	status text,
	constraint fk_meeting
		foreign key(meeting_id)
			references meeting("uuid"),
    constraint fk_user
        foreign key(user_id)
            references "user"(id)
);

create table if not exists recording (
	key_id uuid primary key default uuid_generate_v4(),
	meeting_id text, -- Meeting id
	host_id text, -- user id
	recording_count smallint,
	id text,
	recording_start timestamp,
	recording_end timestamp,
	file_type text,
	file_extension text,
	file_size bigint,
	download_link text,
	recording_type text,
	s3_id text,
	constraint fk_meeting
		foreign key(meeting_id)
			references meeting("uuid"),
	constraint fk_user
		foreign key(host_id)
			references "user"(id)
);