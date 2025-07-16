# lib/tasks/recount_core_likes.rake

namespace :posts do
  desc "Manually recounts core likes from the post_actions table and updates the like_count on the posts table."
  task recount_core_likes: :environment do
    puts "--- Recounting core likes and updating posts.like_count ---"

    # SQL to get the correct like counts for all posts that have likes.
    counts_sql = <<~SQL
      SELECT post_id, COUNT(*) as correct_like_count
      FROM post_actions
      WHERE post_action_type_id = #{PostActionType.types[:like]}
        AND deleted_at IS NULL
      GROUP BY post_id
    SQL

    results = DB.query(counts_sql)
    post_ids_with_likes = results.map(&:post_id)
    puts "Found #{post_ids_with_likes.size} posts with likes to update."

    # Reset like_count for all posts that have likes, to handle cases where likes were removed.
    puts "Resetting like_count for affected posts..."
    Post.where(id: post_ids_with_likes).update_all(like_count: 0)

    # Update posts with the new, correct counts from the SQL query.
    puts "Updating posts with correct like counts..."
    updated_count = 0
    results.each do |row|
      updated_count += Post.where(id: row.post_id).update_all(like_count: row.correct_like_count)
    end

    puts "Core like recount complete. #{updated_count} posts were updated."
  end
end