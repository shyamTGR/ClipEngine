import os
import subprocess
import shutil
import re
from moviepy.editor import VideoFileClip, TextClip, CompositeVideoClip
from multiprocessing import Pool, cpu_count
from clipsai import Transcriber, ClipFinder, MediaEditor, AudioVideoFile, resize
from google.cloud import storage
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

eps=['aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E02 - The One Where Ross Hugs Rachel (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E03 - The One With Ross_ Denial (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E04 - The One Where Joey Loses His Insurance (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E05 - The One With Joey_s Porsche (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E06 - The One On The Last Night (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E07 - The One Where Phoebe Runs (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E08 - The One With Ross_ Teeth (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E09 - The One Where Ross Got High (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E10 - The One With The Routine (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E11 - The One With The Apothecary Table (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E12 - The One With The Joke (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E13 - The One With Rachel_s Sister (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E14 - The One Where Chandler Can_t Cry (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E15 - The One That Could Have Been (1) (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E16 - The One That Could Have Been (2) (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E17 - The One With Unagi (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E18 - The One Where Ross Dates A Student (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E19 - The One With Joey_s Fridge (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E20 - The One With Mac And C.H.E.E.S.E. (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E21 - The One Where Ross Meets Elizabeth_s Dad (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E22 - The One Where Paul_s The Man (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E23 - The One With The Ring (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E24 - The One With The Proposal (1) (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E25 - The One With The Proposal (2) (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_7/Season 7/Friends (1994) - S07E01 - The One With Monica_s Thunder (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_7/Season 7/Friends (1994) - S07E02 - The One With Rachel_s Book (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_7/Season 7/Friends (1994) - S07E03 - The One With Phoebe_s Cookies (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_7/Season 7/Friends (1994) - S07E04 - The One With Rachel_s Assistant (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_7/Season 7/Friends (1994) - S07E05 - The One With The Engagement Picture (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_7/Season 7/Friends (1994) - S07E06 - The One With The Nap Partners (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_7/Season 7/Friends (1994) - S07E07 - The One With Ross_ Library Book (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_7/Season 7/Friends (1994) - S07E08 - The One Where Chandler Doesn_t Like Dogs (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_7/Season 7/Friends (1994) - S07E09 - The One With All The Candy (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_7/Season 7/Friends (1994) - S07E10 - The One With The Holiday Armadillo (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_7/Season 7/Friends (1994) - S07E11 - The One With All The Cheesecakes (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_7/Season 7/Friends (1994) - S07E12 - The One Where They_re Up All Night (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_7/Season 7/Friends (1994) - S07E13 - The One Where Rosita Dies (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_7/Season 7/Friends (1994) - S07E14 - The One Where They All Turn Thirty (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_7/Season 7/Friends (1994) - S07E15 - The One With Joey_s New Brain (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_7/Season 7/Friends (1994) - S07E16 - The One With The Truth About London (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_7/Season 7/Friends (1994) - S07E17 - The One With The Cheap Wedding Dress (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_7/Season 7/Friends (1994) - S07E18 - The One With Joey_s Award (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_7/Season 7/Friends (1994) - S07E19 - The One With Ross And Monica_s Cousin (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_7/Season 7/Friends (1994) - S07E20 - The One With Rachel_s Big Kiss (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_7/Season 7/Friends (1994) - S07E21 - The One With The Vows (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_7/Season 7/Friends (1994) - S07E22 - The One With Chandler_s Dad (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_7/Season 7/Friends (1994) - S07E23 - The One With Chandler And Monica_s Wedding (1) (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_7/Season 7/Friends (1994) - S07E24 - The One With Chandler And Monica_s Wedding (2) (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_8/Season 8/Friends (1994) - S08E01 - The One After I Do (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_8/Season 8/Friends (1994) - S08E02 - The One With The Red Sweater (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_8/Season 8/Friends (1994) - S08E03 - The One Where Rachel Tells Ross (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_8/Season 8/Friends (1994) - S08E04 - The One With The Videotape (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_8/Season 8/Friends (1994) - S08E05 - The One With Rachel_s Date (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_8/Season 8/Friends (1994) - S08E06 - The One With The Halloween Party (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_8/Season 8/Friends (1994) - S08E07 - The One With The Stain (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_8/Season 8/Friends (1994) - S08E08 - The One With The Stripper (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_8/Season 8/Friends (1994) - S08E09 - The One With The Rumor (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_8/Season 8/Friends (1994) - S08E10 - The One With Monica_s Boots (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_8/Season 8/Friends (1994) - S08E11 - The One With The Creepy Holiday Card (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_8/Season 8/Friends (1994) - S08E12 - The One Where Joey Dates Rachel (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_8/Season 8/Friends (1994) - S08E13 - The One Where Chandler Takes A Bath (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_8/Season 8/Friends (1994) - S08E14 - The One With The Secret Closet (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_8/Season 8/Friends (1994) - S08E15 - The One With The Birthing Video (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_8/Season 8/Friends (1994) - S08E16 - The One Where Joey Tells Rachel (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_8/Season 8/Friends (1994) - S08E17 - The One With The Tea Leaves (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_8/Season 8/Friends (1994) - S08E18 - The One In Massapequa (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_8/Season 8/Friends (1994) - S08E19 - The One With Joey_s Interview (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_8/Season 8/Friends (1994) - S08E20 - The One With The Baby Shower (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_8/Season 8/Friends (1994) - S08E21 - The One With The Cooking Class (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_8/Season 8/Friends (1994) - S08E22 - The One Where Rachel Is Late (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_8/Season 8/Friends (1994) - S08E23 - The One Where Rachel Has A Baby (1) (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_8/Season 8/Friends (1994) - S08E24 - The One Where Rachel Has A Baby (2) (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_9/Season 9/Friends (1994) - S09E01 - The One Where No One Proposes (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_9/Season 9/Friends (1994) - S09E02 - The One Where Emma Cries (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_9/Season 9/Friends (1994) - S09E03 - The One With The Pediatrician (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_9/Season 9/Friends (1994) - S09E04 - The One With The Sharks (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_9/Season 9/Friends (1994) - S09E05 - The One With Phoebe_s Birthday Dinner (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_9/Season 9/Friends (1994) - S09E06 - The One With The Male Nanny (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_9/Season 9/Friends (1994) - S09E07 - The One With Ross_ Inappropriate Song (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_9/Season 9/Friends (1994) - S09E08 - The One With Rachel_s Other Sister (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_9/Season 9/Friends (1994) - S09E09 - The One With Rachel_s Phone Number (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_9/Season 9/Friends (1994) - S09E10 - The One With Christmas In Tulsa (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_9/Season 9/Friends (1994) - S09E11 - The One Where Rachel Goes Back To Work (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_9/Season 9/Friends (1994) - S09E12 - The One With Phoebe_s Rats (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_9/Season 9/Friends (1994) - S09E13 - The One Where Monica Sings (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_9/Season 9/Friends (1994) - S09E14 - The One With The Blind Dates (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_9/Season 9/Friends (1994) - S09E15 - The One With The Mugging (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_9/Season 9/Friends (1994) - S09E16 - The One With The Boob Job (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_9/Season 9/Friends (1994) - S09E17 - The One With The Memorial Service (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_9/Season 9/Friends (1994) - S09E18 - The One With The Lottery (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_9/Season 9/Friends (1994) - S09E19 - The One With Rachel_s Dream (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_9/Season 9/Friends (1994) - S09E20 - The One With The Soap Opera Party (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_9/Season 9/Friends (1994) - S09E21 - The One With The Fertility Test (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_9/Season 9/Friends (1994) - S09E22 - The One With The Donor (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_9/Season 9/Friends (1994) - S09E23-E24 - The One In Barbados (1080p BluRay x265 Silence).mp4']

fail_log_path = "fail_log.txt"


def log_failure(vid_name, step, error):
    try:
        with open(fail_log_path, "a", encoding="utf-8") as f:
            f.write(f"[{datetime.utcnow()}] ❌ {vid_name} failed at {step}: {error}\n")
    except Exception as log_err:
        print(f"⚠️ Logging failed for {vid_name} at {step}: {log_err}")


transcriber = Transcriber()
subtitle_transcriber = Transcriber(model_size='large-v2')
total_time_start = time.time()
for e in eps:
  start_time = time.time()
  try:      # -------- CONFIG --------
    pyannote_token = "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"
    bucket_name = "aiclipstraindata"
    gcs_video_path =  e
    gcs_output_folder = "processed_clips"
    
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    # Download input video from GCS
    video_filename = os.path.basename(gcs_video_path)
    bucket.blob(gcs_video_path).download_to_filename(video_filename)
    
    # Safe folder name
    def safe_folder_name(name):
        return re.sub(r'[^\w\-_\. ]', '_', name)
    
    base_title = safe_folder_name(os.path.splitext(video_filename)[0])
    gcs_folder_prefix = f"{gcs_output_folder}/{base_title}/"
    existing_blobs = list(bucket.list_blobs(prefix=gcs_folder_prefix))
    if any(blob.name.endswith(".mp4") for blob in existing_blobs):
        print(f"⚠️ Skipping {base_title} — already processed in GCS.")
        continue

    base_dir = f"output/{base_title}"
    os.makedirs(base_dir, exist_ok=True)
    clip_dir = os.path.join(base_dir, "clips")
    resized_dir = os.path.join(base_dir, "resized")
    final_dir = os.path.join(base_dir, "final")
    
    for directory in [clip_dir, resized_dir, final_dir]:
        os.makedirs(directory, exist_ok=True)
    
    # Transcribe & Find Clips


    transcription = transcriber.transcribe(audio_file_path=video_filename)
    clipfinder = ClipFinder(max_clip_duration=60)
    clips = clipfinder.find_clips(transcription=transcription)

    # Cut and Fix Clip Durations
    def process_clip(args):
        index, clip = args
        try:
            clip_path = os.path.join(clip_dir, f"clip_{index+1}.mp4")
            editor = MediaEditor()
            media = AudioVideoFile(video_filename)
            editor.trim(media, clip.start_time, clip.end_time, clip_path)
            temp = clip.end_time - clip.start_time
            fixed_temp = clip_path.replace(".mp4", "_fixed.mp4")
            subprocess.run(["ffmpeg", "-y", "-i", clip_path, "-t", str(temp), "-c", "copy", fixed_temp],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            os.replace(fixed_temp, clip_path)
            return clip_path
        except Exception as e:
            print(f"❌ Failed to process clip {index+1}: {e}")
            log_failure(base_title, f"clip_{index+1}", e)
            return None
    
    with Pool(min(8, cpu_count())) as pool:
        clip_paths = pool.map(process_clip, enumerate(clips))
    
    # Crop Detection & Resize
    def reencode_and_resize(clip_path):
        try:
            if clip_path is None:
                return None
            fixed_path = clip_path.replace(".mp4", "_cleaned.mp4")
            real_duration = VideoFileClip(clip_path).duration
            subprocess.run([
                "ffmpeg", "-y", "-i", clip_path, "-map", "0:v:0", "-map", "0:a:0",
                "-c:v", "libx264", "-pix_fmt", "yuv420p", "-preset", "veryfast", "-crf", "18",
                "-c:a", "aac", "-ac", "2", "-ar", "44100", "-vf", "fps=23.98", "-t", str(real_duration), fixed_path
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            crops = resize(fixed_path, pyannote_token, aspect_ratio=(9, 16), min_segment_duration=1.0, face_detect_width=128)
            out_path = os.path.join(resized_dir, os.path.basename(fixed_path))
            MediaEditor().resize_video(AudioVideoFile(fixed_path), out_path, crops.crop_width, crops.crop_height, crops.to_dict()["segments"])
            return out_path
        except Exception as e:
            print(f"❌ Failed to reencode and resize {clip_path}: {e}")
            log_failure(base_title, f"clip_{clip_path}", e)
            return None
    

    #Call
    resized_paths = [p for p in (reencode_and_resize(path) for path in clip_paths) if p is not None]
    
    #subtitle_transcriber = transcriber#Transcriber(model_size='large-v2')
    
    # Subtitles & Branding
    def add_subtitles(video_path):
        try:
            if video_path is None:
                return
            output_path = os.path.join(final_dir, os.path.basename(video_path))
            transcription = subtitle_transcriber.transcribe(audio_file_path=video_path)
            video = VideoFileClip(video_path)
            subtitle_clips = []

            for word in transcription.words:
                subtitle = TextClip(txt=word.text.strip(), fontsize=50, font='Vogun-Medium.ttf',
                                    color="Yellow", stroke_color='Yellow', stroke_width=0.5, method='label')
                subtitle = subtitle.set_position(('center', (video.h // 2) + 100)).set_start(word.start_time).set_duration(word.end_time - word.start_time)
                subtitle_clips.append(subtitle)

            logo = TextClip("@thecentralperk", fontsize=25, font='Vogun-Medium.ttf', color="White",
                            stroke_color='White', stroke_width=0.5, method='label').set_duration(video.duration).set_position(('center', (video.h // 2) + 160)).set_opacity(0.7)

            final_video = CompositeVideoClip([video, logo] + subtitle_clips)
            final_video.write_videofile(output_path, codec='libx264', audio_codec='aac', threads=min(4, cpu_count()))

            # Subtitles (.srt)
            srt_lines = []
            for i, word in enumerate(transcription.words, 1):
                start = word.start_time
                end = word.end_time
                text = word.text.strip()

                start_srt = str(datetime.utcfromtimestamp(start).strftime('%H:%M:%S,%f')[:-3])
                end_srt = str(datetime.utcfromtimestamp(end).strftime('%H:%M:%S,%f')[:-3])
                srt_lines.append(f"{i}\n{start_srt} --> {end_srt}\n{text}\n")

            base_name = os.path.splitext(os.path.basename(video_path))[0]
            srt_filename = f"{base_name}.srt"
            srt_local_path = os.path.join(final_dir, srt_filename)
            with open(srt_local_path, "w", encoding="utf-8") as f:
                f.write("\n".join(srt_lines))

            # Upload SRT
            gcs_srt_path = f"{gcs_output_folder}/{base_title}/{srt_filename}"
            try:
                bucket.blob(gcs_srt_path).upload_from_filename(srt_local_path)
                print(f"📝 Uploaded subtitles to GCS: {gcs_srt_path}")
            except Exception as e:
                print(f"❌ Upload SRT failed: {e}")
            finally:
                if os.path.exists(srt_local_path):
                    os.remove(srt_local_path)

        except Exception as e:
            print(f"❌ Failed to add subtitles to {video_path}: {e}")
            log_failure(base_title, f"subtitle_{video_path}", e)
    
    
    

    #CALLLLLLL
    with ThreadPoolExecutor(max_workers=4) as executor:
        subtitle_futures = [
            executor.submit(add_subtitles, path)
            for path in resized_paths
        ]
        for future in as_completed(subtitle_futures):
            try:
                future.result()
            except Exception as e:
                log_failure("unknown", "subtitles", e)
    
    # Upload to GCS
    def upload_to_gcs(local_path, gcs_path):
        try:
            bucket.blob(gcs_path).upload_from_filename(local_path)
            print(f"✅ Uploaded: {os.path.basename(local_path)}")
        except Exception as e:
            print(f"❌ Failed upload: {local_path} — {e}")
            log_failure(base_title, f"clip_{local_path}", e)


    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for filename in os.listdir(final_dir):
            local_path = os.path.join(final_dir, filename)
            gcs_path = f"{gcs_output_folder}/{base_title}/{filename}"
            futures.append(executor.submit(upload_to_gcs, local_path, gcs_path))

    # Optional: wait for all and catch errors
        for future in as_completed(futures):
            _ = future.result()

    # Clean up local files
    shutil.rmtree(base_dir)
    os.remove(video_filename)
    
    print("🎉 All videos processed, uploaded, and local files cleaned up.")
  
  except Exception as ex:
        print(f"❌ Error processing {e}: {ex}")
        try:
            shutil.rmtree(base_dir)
            os.remove(video_filename)
        except Exception as cleanup_ex:
            print(f"⚠️ Cleanup issue: {cleanup_ex}")
            log_failure(base_title, "entire video", ex)
            
        continue
  finally:
        elapsed = time.time() - start_time
        total_elapsed = time.time() - total_time_start
        print(f"⏱️ Time taken for this video: {elapsed:.2f} seconds\n")
        print(f"⏱️ Time taken in total: {total_elapsed:.2f} seconds\n")
        