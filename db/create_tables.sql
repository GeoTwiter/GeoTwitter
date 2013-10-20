/*
	Tables
*/

CREATE TABLE users (
    screen_name character varying,			-- Name from Twitter.
    id bigint NOT NULL,						-- User id from Twitter.
    profile_image_url character varying		-- Link o the last user avatar image.
);

CREATE TABLE posts (
    user_id bigint,							-- User id from Twitter.
    post_id bigint NOT NULL,				-- Post id from Twitter.
    "time" timestamp with time zone,		-- Post creation time from Twitter.
    coordinates_box box,					-- North-West and South-Ost coordinates from Twtter (lat, long).
    coordinates point,						-- Calculate from coordinates_box for maps support.
    text character varying					-- Post text from Twitter.
);

/*
	Owners
*/

ALTER TABLE public.posts OWNER TO postgres;
ALTER TABLE public.users OWNER TO postgres;

/*
	Keys
*/

ALTER TABLE ONLY users
    ADD CONSTRAINT user_pk PRIMARY KEY (id);

ALTER TABLE ONLY posts
    ADD CONSTRAINT posts_pk PRIMARY KEY (post_id);
ALTER TABLE ONLY posts
    ADD CONSTRAINT posts_fk FOREIGN KEY (user_id) REFERENCES users(id);

/*
	Comments
*/

COMMENT ON COLUMN users.screen_name IS 'Name from Twitter.';
COMMENT ON COLUMN users.id IS 'User id from Twitter.';
COMMENT ON COLUMN users.profile_image_url IS 'Link o the last user avatar image.';
COMMENT ON COLUMN posts.user_id IS 'User id from Twitter.';
COMMENT ON COLUMN posts.post_id IS 'Post id from Twitter.';
COMMENT ON COLUMN posts.time IS 'Post creation time from Twitter.';
COMMENT ON COLUMN posts.coordinates_box IS 'North-West and South-Ost coordinates from Twtter (lat, long).';
COMMENT ON COLUMN posts.coordinates IS 'Calculate from coordinates_box for maps support.';
COMMENT ON COLUMN posts.text IS 'Post text from Twitter.';
