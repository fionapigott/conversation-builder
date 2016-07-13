# Function to recursively find all of the children of a tweet
def find_children(root, parent, depth, parent_to_children_map):
    children = []
    children.extend([{"tweet_id": root, "depth": depth, "in_reply_to": parent}])
    # get the children, if there are no children, get an empty list
    p_to_c_dict = parent_to_children_map.get(root, {"children":[]})
    for child in p_to_c_dict["children"]:
        try:
            children.extend(find_children(child, root, depth + 1, parent_to_children_map))
        except KeyError:
            children.extend([{"tweet_id": child, "in_reply_to": root, "depth": depth + 1}])
            pass
    return children