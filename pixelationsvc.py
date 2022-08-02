import asyncio
import datetime
import mimetypes
import tornado.web
import tempfile
import os
import sys
from bson.objectid import ObjectId
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from shutil import disk_usage, copyfileobj
from ..entities.mediainfo import MediaInfo
from ..entities.detectioninfo import DetectionInfo
from ..entities.sessioninfo import SessionInfo
from .webservicehandler import *
from ...server import app
import json
import shutil



supported_mime_types = ['image/jpeg', 'video/mpeg', 'video/mp4']



def parse_number(val: str):
    """Supports locales that use a dot and comma."""
    if not val:
        return 0

    if isinstance(val, (float, int)):
        return val

    if ',' in val:
        if '.' in val:
            val = val.replace(',', '')
        else:
            val = val.replace(',', '.')
    return float(val)





def is_dir_empty(dir_path: Path) -> bool:
    if not dir_path.is_dir():
        return False
    for _ in dir_path.iterdir():
        return False

    return True

LEGACY_ATTRIBUTES_SERIALIZATION = True


class PixelationService(WebServiceHandler):
    def initialize(self, media_db, detections_db,storage_dir, max_request_size, cloud_storage):

        if media_db is None:
            raise Exception("Missing keyword argument 'media_db'.")
        self._mediadb = media_db

        if detections_db is None:
            raise Exception("Missing keyword argument 'detections_db'.")
        self._detections_db = detections_db
        self._storage_dir = storage_dir
        self._cloud_storage = cloud_storage

        if not mimetypes.inited:
            mimetypes.init()

    async def get(self, *args, **kwargs):
        await dispatch_request_async(self, *args, **kwargs)    
        
    async def post(self, *args, **kwargs):
        await dispatch_request_async(self, *args, **kwargs) 
        

    async def webapi_get_item_to_pixelate(self, *args, **kwargs):
        # Retrieve and bundle content to be pixulated
        if not authenticate_user(self):
            self.write(err_response('Authentication failed', 'LOGIN_ERROR'))
            return
        
        self.set_header('Cache-Control', 'no-store, must-revalidate')
        self.set_header('Pragma', 'no-cache')
        self.set_header('Expires', '0')
        
        
        # Get file like image from Azure and send it to the client
        item = None
        for i in app.Database.media.get_media_to_pixelate():
            item = i
            if item is not None:
                if "pixelation_checked_out" in item.attributes:
                    timeout = datetime.datetime.utcnow() - datetime.timedelta(minutes=15)
                    if item.attributes['pixelation_checked_out'] > timeout:
                        item = None
                    else:
                        item.attributes.pop("pixelation_checked_out")
                        break
                else:
                    break

        # Check that a item to pixilate was found, if not then assume nothing left to dot
        if item == None:
            self.set_status(404)
            self.write('Nothing to pixelate')
            return
            
        
        #Download and get bytes from Azure of the media then send bytes
        image_bytes = None
        
                
        try:
            with app.open_media_file(item) as f:
                self.set_header('Attributes', json.dumps(item.attributes))
                self.set_header('Media_ID', json.dumps(str(item.ID)))
                self.set_header('Media_type', json.dumps(item.media_type))
                
                item.attributes['pixelation_checked_out'] = datetime.datetime.utcnow()
                self._mediadb.update(item)
                
                if item.mime_type == 'image/jpeg':
                    fd, src = tempfile.mkstemp('.jpg')
                    os.close(fd)
                    with open(src, 'w+b') as f_out:
                        shutil.copyfileobj(f, f_out)
                    with open(src, 'rb') as f_out:
                        image_bytes = f_out.read()
                        self.set_header('Content-Type', 'image/jpeg')
                        self.set_header('Content-Disposition', 'attachment; filename=source.jpg')
                        self.write(image_bytes)
                        self.flush()
                        os.remove(f_out)
                        
                elif item.mime_type == 'video/mp4':
                    fd, src = tempfile.mkstemp('.mp4')
                    os.close(fd)
                    with open(src, 'w+b') as f_out:
                        shutil.copyfileobj(f, f_out)
                    with open(src, 'rb') as f_out:
                        image_bytes = f_out.read()
                        self.set_header('Content-Type', 'video/mp4')
                        self.set_header('Content-Disposition', 'attachment; filename=source.mp4')
                        self.write(image_bytes)
                        self.flush()
                        os.remove(f_out)
                        
                elif item.mime_type == 'video/mpeg':
                    fd, src = tempfile.mkstemp('.mpeg')
                    os.close(fd)
                    with open(src, 'w+b') as f_out:
                        shutil.copyfileobj(f, f_out)
                    with open(src, 'rb') as f_out:
                        image_bytes = f_out.read()
                        self.set_header('Content-Type', 'video/mpeg')
                        self.set_header('Content-Disposition', 'attachment; filename=source.mpeg')
                        self.write(image_bytes)
                        self.flush()
                        os.remove(f_out)
                else:
                    self.write(err_response(f'Content not in a supported format: {item.ID} is in format {item.mime_type}'))
                    return
                
        except Exception as e:
                self.set_header('Content-Type', 'text/html')
                self.write(f"Error: {e}")

        
    async def webapi_set_item_to_pixelate(self, *args, **kwargs):
        # Retrieve and bundle content to be pixulated
        if not authenticate_user(self):
            self.write(err_response('Authentication failed', 'LOGIN_ERROR'))
            return
        try:
            file = self.request.files['upload_file'][0]
            
            # Get information about source image
            record_id = file['filename']
            source_record = app.Database.media.get(record_id)
            if source_record.media_type == "source_pixelated":
                i = MediaInfo()
                i.ID = None
                
                i.mime_type = source_record.mime_type
                i.media_type = source_record.media_type
                i.item_id = source_record.item_id
                i.offset = source_record.offset
                i.preferred = source_record.preferred
                i.size = int(source_record.size) if source_record.size is not None else None
                i.attributes = source_record.attributes
                if i.mime_type is not None or i.mime_type is not "image/jpeg":
                    i.duration = source_record.duration

                if source_record.selection_start is not None:
                    i.selection_start = source_record.selection_start

                if source_record.selection_stop is not None:
                    i.selection_stop = source_record.selection_stop
                    
                source_record.attributes['pixelation_required'] = False
                #Add Censored Record to Database    
                record_id = self._mediadb.add(i)
            else:
                # Make a new record with the same information as the pixelated (new ID)
                i = MediaInfo()
                i.ID = None
                
                i.mime_type = source_record.mime_type
                i.media_type = source_record.media_type + "_pixelated"
                i.item_id = source_record.item_id
                i.offset = source_record.offset
                i.preferred = source_record.preferred
                i.size = int(source_record.size) if source_record.size is not None else None
                i.attributes = source_record.attributes
                if i.mime_type is not None or i.mime_type is not "image/jpeg":
                    i.duration = source_record.duration

                if source_record.selection_start is not None:
                    i.selection_start = source_record.selection_start

                if source_record.selection_stop is not None:
                    i.selection_stop = source_record.selection_stop
                    
                source_record.attributes['pixelation_required'] = False
                #Add Censored Record to Database    
                record_id = self._mediadb.add(i)
            
            #Add Azure path to media entry
            if self._cloud_storage is not None:
                #-------------CLOUD_STORAGE --------------
                i.file_path = self._cloud_storage.get_owner_url(i)
                    
            self._mediadb.update(i)
            
            #Make Media Temp File to upload
            if source_record.mime_type == 'image/jpeg':
                fd, src = tempfile.mkstemp('.jpg')
                os.close(fd)
                with open(src, 'w+b') as f_out:
                    f_out.write(file['body'])

            elif source_record.mime_type == 'video/mp4':
                fd, src = tempfile.mkstemp('.mp4')
                os.close(fd)
                with open(src, 'w+b') as f_out:
                    f_out.write(file['body'])

            elif source_record.mime_type == 'video/mpeg':
                fd, src = tempfile.mkstemp('.mpeg')
                os.close(fd)
                with open(src, 'w+b') as f_out:
                    f_out.write(file['body'])

            self._tmp_file = src
            
            # Upload Image to azure
            # Transfer the file.
            def work():
                if self._cloud_storage is not None:
                    #CLOUD STORAGE
                    self._cloud_storage.upload_media(i, self._tmp_file)

            await IOLoop.current().run_in_executor(None, work)
            
            #Remove file from local server
            os.remove(self._tmp_file)
            
            # Update source to reflect it no longer needs to be pixilated
            if source_record.media_type == "source_pixelated":
                self._mediadb.remove(source_record)
            else:
                self._mediadb.update(source_record)
            
            self.write(ok_response())
            
        except Exception as e:
                self.set_header('Content-Type', 'text/html')
                self.write(f"Error: {e}")
