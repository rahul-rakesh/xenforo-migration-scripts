# frozen_string_literal: true

#################################
# ------ CONFIGURATION ------ #
#################################

# 1. For a final check, run with DRY_RUN = true and LIMIT = nil to see all affected posts.
# 2. To run a small batch, set DRY_RUN = false and LIMIT = 50.
# 3. For the final bulk update, set DRY_RUN = false and LIMIT = nil.
DRY_RUN = false

# Set to nil to run on all qualifying posts.
TARGET_POST_ID = nil

# Set to a number (e.g., 50) for a batch, or nil for all posts.
LIMIT = nil

# The minimum number of consecutive images to trigger the grid wrap.
MIN_IMAGES = 3

#################################
# ------ SCRIPT LOGIC (v2 - Production) ------ #
#################################

puts "Starting image grid wrapper script (v2)..."
puts "---"
puts "MODE: #{DRY_RUN ? 'DRY RUN' : 'LIVE RUN'}"
puts "Limit: #{LIMIT || 'All Posts'}"
puts "---"

# Regex to find an OPTIONAL legacy "Attachments" header and a required block of images.
regex = /(?:<hr>\s*<strong>Attachments:<\/strong>\s*)?((?:!\[[^\]]*?\]\(upload:\/\/[^\)]+?\)\s*){#{MIN_IMAGES},})/m

# Find posts that have uploads but do not already have a [grid] tag.
posts = Post.where("raw ILIKE '%upload://%' AND raw NOT ILIKE '%[grid]%'")

posts = posts.where(id: TARGET_POST_ID) if TARGET_POST_ID
posts = posts.limit(LIMIT) if LIMIT

processed_count = 0
changed_count = 0

posts.find_each do |post|
  processed_count += 1
  original_raw = post.raw.dup

  modified_raw = original_raw.gsub(regex) do
    # $1 contains the matched block of images.
    image_block = $1
    "\n[grid]\n#{image_block.strip}\n[/grid]\n"
  end

  if original_raw != modified_raw
    changed_count += 1
    puts "✅ Change required in Post ID: #{post.id} (Topic: #{post.topic_id})"

    if DRY_RUN
      puts "   (Dry Run - No changes made)"
    else
      begin
        revisor = PostRevisor.new(post)
        revisor.revise!(
          Discourse.system_user,
          { raw: modified_raw },
          { bypass_bump: true }
        )
        puts "   ✅ Post ID: #{post.id} successfully updated."
      rescue => e
        puts "   ❌ ERROR updating Post ID: #{post.id}. Reason: #{e.message}"
      end
    end
  end
end

puts "---"
puts "Script finished."
puts "Total posts scanned: #{processed_count}"
puts "Posts requiring changes: #{changed_count}"
puts "Mode: #{DRY_RUN ? 'DRY RUN - No changes were saved.' : 'LIVE RUN - Changes were saved.'}"