CREATE TABLE IF NOT EXISTS public.education_dialogue (
  topic TEXT NOT NULL,
  student_prefrences TEXT NOT NULL,
  teacher_prefrences TEXT NOT NULL,
  student_reactions TEXT NOT NULL,
  teacher_reactions TEXT NOT NULL,
  conversation JSONB NOT NULL
);
