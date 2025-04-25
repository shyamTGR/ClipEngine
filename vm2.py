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

eps=['aiclipsgen/Train/training_data/Season_2/Season 2/Friends (1994) - S02E04 - The One With Phoebe_s Husband (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_2/Season 2/Friends (1994) - S02E05 - The One With Five Steaks And An Eggplant (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_2/Season 2/Friends (1994) - S02E06 - The One With The Baby On The Bus (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_2/Season 2/Friends (1994) - S02E07 - The One Where Ross Finds Out (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_2/Season 2/Friends (1994) - S02E08 - The One With The List (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_2/Season 2/Friends (1994) - S02E09 - The One With Phoebe_s Dad (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_2/Season 2/Friends (1994) - S02E10 - The One With Russ (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_2/Season 2/Friends (1994) - S02E11 - The One With The Lesbian Wedding (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_2/Season 2/Friends (1994) - S02E12 - The One After The Super Bowl (1) (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_2/Season 2/Friends (1994) - S02E13 - The One After The Super Bowl (2) (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_2/Season 2/Friends (1994) - S02E14 - The One With The Prom Video (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_2/Season 2/Friends (1994) - S02E15 - The One Where Ross And Rachel.You Know (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_2/Season 2/Friends (1994) - S02E16 - The One Where Joey Moves Out (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_2/Season 2/Friends (1994) - S02E17 - The One Where Eddie Moves In (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_2/Season 2/Friends (1994) - S02E18 - The One Where Dr. Ramoray Dies (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_2/Season 2/Friends (1994) - S02E19 - The One Where Eddie Won_t Go (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_2/Season 2/Friends (1994) - S02E20 - The One Where Old Yeller Dies (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_2/Season 2/Friends (1994) - S02E21 - The One With The Bullies (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_2/Season 2/Friends (1994) - S02E22 - The One With The Two Parties (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_2/Season 2/Friends (1994) - S02E23 - The One With The Chicken Pox (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_2/Season 2/Friends (1994) - S02E24 - The One With Barry And Mindy_s Wedding (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E01 - The One With The Princess Leia Fantasy (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E02 - The One Where No One_s Ready (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E03 - The One With The Jam (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E04 - The One With The Metaphorical Tunnel (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E05 - The One With Frank Jr. (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E06 - The One With The Flashback (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E07 - The One With The Race Car Bed (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E08 - The One With The Giant Poking Device (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E09 - The One With The Football (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E10 - The One Where Rachel Quits (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E11 - The One Where Chandler Can_t Remember Which Sister (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E12 - The One With All The Jealousy (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E13 - The One Where Monica And Richard Are Just Friends (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E14 - The One With Phoebe_s Ex-Partner (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E15 - The One Where Ross And Rachel Take A Break (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E16 - The One On The Morning After (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E17 - The One Without The Ski Trip (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E18 - The One With The Hypnosis Tape (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E19 - The One With The Tiny T-Shirt (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E20 - The One With The Dollhouse (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E21 - The One With A Chick And A Duck (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E22 - The One With The Screamer (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E23 - The One With Ross_ Thing (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E24 - The One With The Ultimate Fighting Champion (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_3/Season 3/Friends (1994) - S03E25 - The One At The Beach (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_4/Season 4/Friends (1994) - S04E01 - The One With The Jellyfish (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_4/Season 4/Friends (1994) - S04E02 - The One With The Cat (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_4/Season 4/Friends (1994) - S04E03 - The One With The _Cuffs (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_4/Season 4/Friends (1994) - S04E04 - The One With The Ballroom Dancing (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_4/Season 4/Friends (1994) - S04E05 - The One With Joey_s New Girlfriend (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_4/Season 4/Friends (1994) - S04E06 - The One With The Dirty Girl (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_4/Season 4/Friends (1994) - S04E07 - The One Where Chandler Crosses The Line (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_4/Season 4/Friends (1994) - S04E08 - The One With Chandler In A Box (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_4/Season 4/Friends (1994) - S04E09 - The One Where They_re Going To Party! (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_4/Season 4/Friends (1994) - S04E10 - The One With The Girl From Poughkeepsie (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_4/Season 4/Friends (1994) - S04E11 - The One With Phoebe_s Uterus (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_4/Season 4/Friends (1994) - S04E12 - The One With The Embryos (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_4/Season 4/Friends (1994) - S04E13 - The One With Rachel_s Crush (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_4/Season 4/Friends (1994) - S04E14 - The One With Joey_s Dirty Day (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_4/Season 4/Friends (1994) - S04E15 - The One With All The Rugby (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_4/Season 4/Friends (1994) - S04E16 - The One With The Fake Party (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_4/Season 4/Friends (1994) - S04E17 - The One With The Free Porn (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_4/Season 4/Friends (1994) - S04E18 - The One With Rachel_s New Dress (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_4/Season 4/Friends (1994) - S04E19 - The One With All The Haste (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_4/Season 4/Friends (1994) - S04E20 - The One With All The Wedding Dresses (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_4/Season 4/Friends (1994) - S04E21 - The One With The Invitation (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_4/Season 4/Friends (1994) - S04E22 - The One With The Worst Best Man Ever (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_4/Season 4/Friends (1994) - S04E23 - The One With Ross_s Wedding (1) (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_4/Season 4/Friends (1994) - S04E24 - The One With Ross_s Wedding (2) (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_5/Season 5/Friends (1994) - S05E01 - The One After Ross Says Rachel (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_5/Season 5/Friends (1994) - S05E02 - The One With All The Kissing (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_5/Season 5/Friends (1994) - S05E03 - The One Hundredth (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_5/Season 5/Friends (1994) - S05E04 - The One Where Phoebe Hates PBS (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_5/Season 5/Friends (1994) - S05E05 - The One With The Kips (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_5/Season 5/Friends (1994) - S05E06 - The One With The Yeti (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_5/Season 5/Friends (1994) - S05E07 - The One Where Ross Moves In (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_5/Season 5/Friends (1994) - S05E08 - The One With The Thanksgiving Flashbacks (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_5/Season 5/Friends (1994) - S05E09 - The One With Ross_ Sandwich (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_5/Season 5/Friends (1994) - S05E10 - The One With The Inappropriate Sister (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_5/Season 5/Friends (1994) - S05E11 - The One With All The Resolutions (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_5/Season 5/Friends (1994) - S05E12 - The One With Chandler_s Work Laugh (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_5/Season 5/Friends (1994) - S05E13 - The One With Joey_s Bag (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_5/Season 5/Friends (1994) - S05E14 - The One Where Everybody Finds Out (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_5/Season 5/Friends (1994) - S05E15 - The One With The Girl Who Hits Joey (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_5/Season 5/Friends (1994) - S05E16 - The One With The Cop (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_5/Season 5/Friends (1994) - S05E17 - The One With Rachel_s Inadvertent Kiss (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_5/Season 5/Friends (1994) - S05E18 - The One Where Rachel Smokes (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_5/Season 5/Friends (1994) - S05E19 - The One Where Ross Can_t Flirt (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_5/Season 5/Friends (1994) - S05E20 - The One With The Ride Along (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_5/Season 5/Friends (1994) - S05E21 - The One With The Ball (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_5/Season 5/Friends (1994) - S05E22 - The One With Joey_s Big Break (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_5/Season 5/Friends (1994) - S05E23 - The One In Vegas (1) (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_5/Season 5/Friends (1994) - S05E24 - The One In Vegas (2) (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_6/Season 6/Friends (1994) - S06E01 - The One After Vegas (1080p BluRay x265 Silence).mp4']

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
        