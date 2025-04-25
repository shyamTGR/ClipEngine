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

eps=['aiclipsgen/Train/training_data/Featurettes/Featurettes/Bonus Disc/Friends Visits The Ellen DeGeneres Show_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Bonus Disc/Friends from the Start_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Bonus Disc/Friends on The Tonight Show with Jay Leno_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Bonus Disc/Gag Reel -The One with Never-Before-Seen Gags_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Bonus Disc/Music Video - I_ll Be There for You by The Rembrandts_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Bonus Disc/The Legacy of Friends_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Bonus Disc/The One Where Rachel Tells Ross - Original Producer_s Cut_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Bonus Disc/The Original Script - The One Where Rachel Tells Ross (149 pages)_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Bonus Disc/When Friends Become Family_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 1/Friends of Friends Clips_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 1/Trailer - The One with the Trailer of Season 2_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 10/Friends Final Thoughts_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 10/Friends of Friends_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 10/Gag Reels S01-04_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 10/Gag Reels S10_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 10/Joey Joey - Music Video_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 2/Friends of Friends_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 2/Smelly Cat Music Video_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 2/What_s Up with Your Friends - Chandler_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 2/What_s Up with Your Friends - Gunther_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 2/What_s Up with Your Friends - Joey_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 2/What_s Up with Your Friends - Monica_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 2/What_s Up with Your Friends - Phoebe_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 2/What_s Up with Your Friends - Rachel_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 2/What_s Up with Your Friends - Ross_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 3/_Friends of _Friends__ clips_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 3/_What_s Up with Your Friends__new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 4/Friends Around the World_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 4/_Friends of _Friends___new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 4/_What_s Up with Your Friends__new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 5/Friends - On Location in London_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 5/Gunther Spills the Beans_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 5/The One That Goes Behind the Scenes_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 6/Friends of Friends_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 6/Gag Reel_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 6/Gunther Spills the Beans_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 7/Extended Episodes - S07E13 - The One Where Rosita Dies_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 7/Extended Episodes - S07E14 - The One Where They All Turn Thirty_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 7/Extended Episodes - S07E15 - The One with Joey_s New Brain_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 7/Extended Episodes - S07E16 - The One with the Truth About London_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 7/Friends of Friends_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 7/Gag Reel_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 7/Gunther Spills the Beans_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 8/Friends of Friends_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 8/Gag Reel_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 8/Gunther Spills the Beans_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 9/Behind the Style - The Look of Friends_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 9/Gag Reel_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 9/Gunther Spills the Beans_new.mp4',
 'aiclipsgen/Train/training_data/Featurettes/Featurettes/Season 9/Phoebe Battles the Pink Robots - Music video by The Flaming Lips_new.mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Copy of Friends (1994) - S01E02 - The One With The Sonogram At The End (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Friends (1994) - S01E01 - The One Where Monica Gets A Roommate (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Friends (1994) - S01E02 - The One With The Sonogram At The End (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Friends (1994) - S01E03 - The One With The Thumb (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Friends (1994) - S01E04 - The One With George Stephanopoulos (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Friends (1994) - S01E05 - The One With The East German Laundry Detergent (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Friends (1994) - S01E06 - The One With The Butt (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Friends (1994) - S01E07 - The One With The Blackout (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Friends (1994) - S01E08 - The One Where Nana Dies Twice (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Friends (1994) - S01E09 - The One Where Underdog Gets Away (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Friends (1994) - S01E10 - The One With The Monkey (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Friends (1994) - S01E11 - The One With Mrs. Bing (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Friends (1994) - S01E12 - The One With The Dozen Lasagnas (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Friends (1994) - S01E13 - The One With The Boobies (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Friends (1994) - S01E14 - The One With The Candy Hearts (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Friends (1994) - S01E15 - The One With The Stoned Guy (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Friends (1994) - S01E16 - The One With Two Parts (1) (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Friends (1994) - S01E17 - The One With Two Parts (2) (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Friends (1994) - S01E18 - The One With All The Poker (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Friends (1994) - S01E19 - The One Where The Monkey Gets Away (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Friends (1994) - S01E20 - The One With The Evil Orthodontist (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Friends (1994) - S01E21 - The One With The Fake Monica (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Friends (1994) - S01E22 - The One With The Ick Factor (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Friends (1994) - S01E23 - The One With The Birth (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_1/Season 1/Friends (1994) - S01E24 - The One Where Rachel Finds Out (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_10/Season 10/Friends (1994) - S10E01 - The One After Joey And Rachel Kiss (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_10/Season 10/Friends (1994) - S10E02 - The One Where Ross Is Fine (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_10/Season 10/Friends (1994) - S10E03 - The One With Ross_ Tan (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_10/Season 10/Friends (1994) - S10E04 - The One With The Cake (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_10/Season 10/Friends (1994) - S10E05 - The One Where Rachel_s Sister Babysits (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_10/Season 10/Friends (1994) - S10E06 - The One With Ross_ Grant (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_10/Season 10/Friends (1994) - S10E07 - The One With The Home Study (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_10/Season 10/Friends (1994) - S10E08 - The One With The Late Thanksgiving (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_10/Season 10/Friends (1994) - S10E09 - The One With The Birth Mother (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_10/Season 10/Friends (1994) - S10E10 - The One Where Chandler Gets Caught (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_10/Season 10/Friends (1994) - S10E11 - The One Where The Stripper Cries (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_10/Season 10/Friends (1994) - S10E12 - The One With Phoebe_s Wedding (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_10/Season 10/Friends (1994) - S10E13 - The One Where Joey Speaks French (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_10/Season 10/Friends (1994) - S10E14 - The One With Princess Consuela (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_10/Season 10/Friends (1994) - S10E15 - The One Where Estelle Dies (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_10/Season 10/Friends (1994) - S10E16 - The One With Rachel_s Going Away Party (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_10/Season 10/Friends (1994) - S10E17-E18 - The Last One (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_2/Season 2/Friends (1994) - S02E01 - The One With Ross_s New Girlfriend (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_2/Season 2/Friends (1994) - S02E02 - The One With The Breast Milk (1080p BluRay x265 Silence).mp4',
 'aiclipsgen/Train/training_data/Season_2/Season 2/Friends (1994) - S02E03 - The One Where Heckles Dies (1080p BluRay x265 Silence).mp4']


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
        