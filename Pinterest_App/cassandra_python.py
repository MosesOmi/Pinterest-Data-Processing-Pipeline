from cassandra.cluster import Cluster

new_cluster = Cluster()
session = new_cluster.connect('pinterest')

session.execute(''' CREATE TABLE pin_data (
    category text,
    "index" text,
    unique_id text,
    title text,
    description text,
    follower_count text,
    tag_list text,
    is_image_or_video text,
    image_src text,
    save_location text,
    PRIMARY KEY (unique_id)
);''')
