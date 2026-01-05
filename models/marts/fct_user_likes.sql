version: 2

models:
  - name: fct_user_likes
    description: >
      Fact table of user like events. Each row represents a single like action
      from one user to another, including where it happened in the app and when.

    columns:
      - name: like_id
        description: "Unique identifier for the like event (table grain: 1 row per like_id)."
        tests:
          - not_null
          - unique

      - name: liker_user_id
        description: "User ID of the person who initiated the like (sender)."
        tests:
          - not_null

      - name: liked_user_id
        description: "User ID of the person who received the like (receiver)."
        tests:
          - not_null
          # - relationships:
          #     to: ref('dim_users')
          #     field: user_id

      - name: like_type
        description: "Type of like event (e.g. 'like', 'rose', 'super_like')."
        tests:
          # adjust values when you know the real enums
          - accepted_values:
              values: ['like', 'rose', 'super_like']

      - name: source_surface
        description: "App surface where the like occurred (e.g. 'discovery', 'standouts', 'profile')."
        tests:
          - accepted_values:
              values: ['discovery', 'standouts', 'profile']

      - name: liked_at
        description: "Timestamp when the like occurred in the app."
        tests:
          - not_null

      - name: idempotency_key
        description: "Idempotency key from the source system used to avoid processing the same like multiple times."

      - name: comment_text
        description: "Optional text comment or message that accompanied the like, if any."
