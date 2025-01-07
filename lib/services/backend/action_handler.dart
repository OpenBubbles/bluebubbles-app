import 'dart:async';
import 'dart:convert';

import 'package:bluebubbles/helpers/ui/facetime_helpers.dart';
import 'package:bluebubbles/database/models.dart';
import 'package:bluebubbles/services/services.dart';
import 'package:bluebubbles/helpers/helpers.dart';
import 'package:bluebubbles/utils/file_utils.dart';
import 'package:bluebubbles/services/rustpush/rustpush_service.dart';
import 'package:bluebubbles/services/network/backend_service.dart';
import 'package:bluebubbles/utils/logger/logger.dart';
import 'package:collection/collection.dart';
import 'package:dio/dio.dart';
import 'package:ffmpeg_kit_flutter/ffmpeg_kit.dart';
import 'package:ffmpeg_kit_flutter/ffprobe_kit.dart';
import 'package:ffmpeg_kit_flutter/log.dart';
import 'package:ffmpeg_kit_flutter/return_code.dart';
import 'package:ffmpeg_kit_flutter/session.dart';
import 'package:ffmpeg_kit_flutter/statistics.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:get/get.dart' hide Response;
import 'package:mime_type/mime_type.dart';
import 'package:tuple/tuple.dart';
import 'package:universal_io/io.dart';

ActionHandler ah = Get.isRegistered<ActionHandler>() ? Get.find<ActionHandler>() : Get.put(ActionHandler());

class ActionHandler extends GetxService {
  final RxList<Tuple2<String, RxDouble>> attachmentProgress = <Tuple2<String, RxDouble>>[].obs;
  final List<String> outOfOrderTempGuids = [];
  final List<String> handledNewMessages = [];
  CancelToken? latestCancelToken;

  /// Checks if a GUID has been handled.
  /// After each check, before returning, trim the list of GUIDs to the last 100.
  bool shouldNotifyForNewMessageGuid(String guid) {
    if (handledNewMessages.contains(guid)) return false;
    handledNewMessages.add(guid);

    if (handledNewMessages.length > 100) {
      handledNewMessages.removeRange(0, handledNewMessages.length - 100);
    }

    return true;
  }
  
  Future<List<Message>> prepMessage(Chat c, Message m, Message? selected, String? r, {bool clearNotificationsIfFromMe = true}) async {
    if ((m.text?.isEmpty ?? true) && (m.subject?.isEmpty ?? true) && r == null) return [];

    final List<Message> messages = <Message>[];

    if (!(await ss.isMinBigSur) && r == null) {
      // Split URL messages on OS X to prevent message matching glitches
      String mainText = m.text!;
      String? secondaryText;
      final match = parseLinks(m.text!.replaceAll("\n", " ")).firstOrNull;
      if (match != null) {
        if (match.start == 0) {
          mainText = m.text!.substring(0, match.end).trimRight();
          secondaryText = m.text!.substring(match.end).trimLeft();
        } else if (match.end == m.text!.length) {
          mainText = m.text!.substring(0, match.start).trimRight();
          secondaryText = m.text!.substring(match.start).trimLeft();
        }
      }

      messages.add(m..text = mainText);
      if (!isNullOrEmpty(secondaryText)) {
        messages.add(Message(
          text: secondaryText,
          threadOriginatorGuid: m.threadOriginatorGuid,
          threadOriginatorPart: "${m.threadOriginatorPart ?? 0}:0:0",
          expressiveSendStyleId: m.expressiveSendStyleId,
          dateCreated: DateTime.now(),
          hasAttachments: false,
          isFromMe: true,
          handleId: 0,
        ));
      }

      for (Message message in messages) {
        message.generateTempGuid();
        await c.addMessage(message, clearNotificationsIfFromMe: clearNotificationsIfFromMe);
      }
    } else {
      m.generateTempGuid();
      await c.addMessage(m, clearNotificationsIfFromMe: clearNotificationsIfFromMe);
      messages.add(m);
    }
    return messages;
  }

  Future<void> matchMessageWithExisting(Chat chat, String existingGuid, Message replacement, {Message? existing}) async {
    // First, try to find a matching message with the replacement's GUID.
    // We check this first because if an event came in for that GUID, we should be able to ignore
    // the API response.
    final existingReplacementMessage = Message.findOne(guid: replacement.guid);
    if (existingReplacementMessage != null) {
      Logger.debug("Found existing message with GUID ${replacement.guid}...", tag: "MessageStatus");

      // if we are the source of truth (eg there is no remote omniscient DB) partial updates are good and intended. Always do them.
      if (backend.getRemoteService() == null || replacement.isNewerThan(existingReplacementMessage)) {
        Logger.debug("Replacing existing message with newer message (GUID: ${replacement.guid})...", tag: "MessageStatus");
        await Message.replaceMessage(replacement.guid, replacement);
      } else {
        Logger.debug("Existing message with GUID ${replacement.guid} is newer than the replacement...", tag: "MessageStatus");
      }
      
      // Delete the temp message if it exists
      if (existingGuid != replacement.guid) {
        Logger.debug("Deleting temp message with GUID $existingGuid...", tag: "MessageStatus");
        final existingTempMessage = Message.findOne(guid: existingGuid);
        if (existingTempMessage != null) {
          Message.delete(existingTempMessage.guid!);
          if (existing != null) {
            ms(chat.guid).removeMessage(existing);
          }
        }
      }
    } else {
      try {
        // If we didn't find a matching message with the replacement's GUID, replace the existing message.
      Logger.debug("Replacing message with GUID $existingGuid with ${replacement.guid}...", tag: "MessageStatus");
      await Message.replaceMessage(existingGuid, replacement);
      } catch (ex) {
        Logger.warn("Unable to find & replace message with GUID $existingGuid...", tag: "MessageStatus", error: ex);
      }
    }
  }

  Future<void> matchAttachmentWithExisting(Chat chat, String existingGuid, Attachment replacement, {Attachment? existing}) async {
    // First, try to find a matching message with the replacement's GUID.
    // We check this first because if an event came in for that GUID, we should be able to ignore
    // the API response.
    final existingReplacementMessage = Attachment.findOne(replacement.guid!);
    if (existingReplacementMessage != null) {
      Logger.debug("Replacing existing attachment with GUID ${replacement.guid}...", tag: "AttachmentStatus");
      await Attachment.replaceAttachment(replacement.guid, replacement);
      
      // Delete the temp message if it exists
      if (existingGuid != replacement.guid) {
        Logger.debug("Deleting temp attachment with GUID $existingGuid...", tag: "AttachmentStatus");
        final existingTempMessage = Attachment.findOne(existingGuid);
        if (existingTempMessage != null) {
          Attachment.delete(existingTempMessage.guid!);
        }
      }
    } else {
      try {
        Logger.debug("Replacing original attachment with GUID $existingGuid with ${replacement.guid}...", tag: "AttachmentStatus");
        await Attachment.replaceAttachment(existingGuid, replacement);
      } catch (ex) {
        Logger.warn("Unable to find & replace attachment with GUID $existingGuid...", error: ex, tag: "AttachmentStatus");
      }
    }
  }

  Future<void> sendMessage(Chat c, Message m, Message? selected, String? r) async {
    if (c.isArchived!) c.toggleArchived(false);
    final completer = Completer<void>();
    if (r == null) {
      backend.sendMessage(c, m).then((newMessage) async {
        try {
          await matchMessageWithExisting(c, m.guid!, newMessage, existing: m);
        } catch (ex) {
          Logger.warn("Failed to find message match for ${m.guid} -> ${newMessage.guid}!", error: ex, tag: "MessageStatus");
        }

        completer.complete();
      }).catchError((error, stack) async {
        Logger.error('Failed to send message!', error: error, trace: stack);

        final tempGuid = m.guid;
        m = handleSendError(error, m);

        if (!ls.isAlive || !(cm.getChatController(c.guid)?.isAlive ?? false)) {
          await notif.createFailedToSend(c);
        }
        await Message.replaceMessage(tempGuid, m);
        completer.completeError(error);
      });
    } else {
      backend.sendTapback(c, selected!, r, m.associatedMessagePart).then((newMessage) async {
        try {
          await matchMessageWithExisting(c, m.guid!, newMessage, existing: m);
        } catch (ex) {
          Logger.warn("Failed to find message match for ${m.guid} -> ${newMessage.guid}!", error: ex, tag: "MessageStatus");
        }
        completer.complete();
      }).catchError((error, stack) async {
        Logger.error('Failed to send message!', error: error, trace: stack);

        final tempGuid = m.guid;
        m = handleSendError(error, m);

        if (!ls.isAlive || !(cm.getChatController(c.guid)?.isAlive ?? false)) {
          await notif.createFailedToSend(c);
        }
        await Message.replaceMessage(tempGuid, m);
        completer.completeError(error);
      });
    }

    return completer.future;
  }

  Future<void> sendMultipart(Chat c, Message m, Message? selected, String? r) async {
    final completer = Completer<void>();

    List<Map<String, dynamic>> parts = m.attributedBody.first.runs.map((e) => {
      "text": m.attributedBody.first.string.substring(e.range.first, e.range.first + e.range.last),
      "mention": e.attributes!.mention,
      "partIndex": e.attributes!.messagePart,
    }).toList();

    http.sendMultipart(
      c.guid,
      m.guid!,
      parts,
      subject: m.subject,
      selectedMessageGuid: m.threadOriginatorGuid,
      effectId: m.expressiveSendStyleId,
      partIndex: int.tryParse(m.threadOriginatorPart?.split(":").firstOrNull ?? ""),
      ddScan: !ss.isMinSonomaSync && parts.any((e) => e["text"].toString().hasUrl)
    ).then((response) async {
      final newMessage = Message.fromMap(response.data['data']);
      try {
        await matchMessageWithExisting(c, m.guid!, newMessage, existing: m);
      } catch (ex) {
        Logger.warn("Failed to find message match for ${m.guid} -> ${newMessage.guid}!", error: ex, tag: "MessageStatus");
      }
      completer.complete();
    }).catchError((error, stack) async {
      Logger.error('Failed to send message!', error: error, trace: stack);

      final tempGuid = m.guid;
      m = handleSendError(error, m);

      if (!ls.isAlive || !(cm.getChatController(c.guid)?.isAlive ?? false)) {
        await notif.createFailedToSend(c);
      }
      await Message.replaceMessage(tempGuid, m);
      completer.completeError(error);
    });

    return completer.future;
  }
  
  Future<void> prepAttachment(Chat c, Message m) async {
    final attachment = m.attachments.first!;
    final progress = Tuple2(attachment.guid!, 0.0.obs);
    attachmentProgress.add(progress);
    // Save the attachment to storage and DB
    if (!kIsWeb) {
      var dialog = attachment.totalBytes! > 100 * 1000 * 1000;
      if (dialog) {
        showDialog(context: Get.context!,
          barrierDismissible: false,
          builder: (BuildContext context) {
            return AlertDialog(
              title: Text("Preparing...", style: context.theme.textTheme.titleLarge),
              backgroundColor: context.theme.colorScheme.properSurface,
              actions: [],
            );
        });
      }
      
      String directory = "${fs.appDocDir.path}/attachments/${attachment.guid}";
      String pathName = "$directory/${attachment.transferName}";
      final file = await File(pathName).create(recursive: true);
      if (attachment.bytes != null) {
        if (attachment.mimeType == "image/gif") {
          attachment.bytes = await fixSpeedyGifs(attachment.bytes!);
        }
        await file.writeAsBytes(attachment.bytes!);
      } else {
        await File(attachment.sourcePath!).copy(pathName);
      }

      var mm = attachment.mimeType ?? mime(attachment.transferName);
      // 100 MB
      if (dialog && mm != null && mm.startsWith("video/") && Platform.isAndroid) {

        String tempPath = "$directory/temp.mp4";
        file.renameSync(tempPath);
        // use FFMPEG to shrink
        var info = await FFprobeKit.getMediaInformation(tempPath);
        Logger.info(await info.getOutput());
        var output = json.decode((await info.getOutput())!);
        double duration = double.parse(output["format"]["duration"]);
        List<dynamic> streams = output["streams"];
        var videoStream = streams.firstWhereOrNull((stream) => stream["codec_type"] == "video");
        int width = videoStream?["width"] ?? 1;
        int height = videoStream?["height"] ?? 1;
        var rotation = ((videoStream?["side_data_list"] as List<dynamic>?)?.firstOrNull?["rotation"] ?? 0).abs();
        var flip = rotation == 90 || rotation == 270;
        if (flip) {
          var tmp = width;
          width = height;
          height = tmp;
        }
        if (width > height) {
          var aspect = height / width;
          width = 1920;
          height = (aspect * width).floor();
        } else {
          var aspect = width / height;
          height = 1920;
          width = (aspect * height).floor();
        }

        double progress = 0;
        Function? myUpdate;

        Completer c = Completer();

        var audioRate = 128 * 1000;
        var maxBits = (50 * 1000 * 1000 * 8) - (audioRate * duration).floor();
        var bitsPerSecond = (maxBits / duration).floor();
        var executed = await FFmpegKit.executeAsync("-i \"$directory/temp.mp4\" -b:v $bitsPerSecond -vf \"scale=$width:$height\" -c:a aac -b:a $audioRate \"$directory/compressed.mp4\"",
          (Session session) async {
            if (myUpdate != null) {
              Navigator.of(Get.context!).pop(); // dismiss dialog
            }
            c.complete();
            Logger.info("Session finsihed $session");
          },
          (Log log) async { },
          (Statistics statistics) {
            progress = statistics.getTime() / 1000; // ms to s
            if (myUpdate != null) {
              myUpdate!(() {});
            }
            Logger.info("Time $progress");
          });

        Get.back();

        showDialog(context: Get.context!,
          barrierDismissible: false,
          builder: (BuildContext context) {
            return AlertDialog(
              title: Text("Compressing...", style: context.theme.textTheme.titleLarge),
              content: SizedBox(
                      height: 5,
                      child: Center(
                        child: StatefulBuilder(builder: (context, update) {
                          myUpdate = update;
                          return LinearProgressIndicator(
                            value: progress / duration,
                            backgroundColor: context.theme.colorScheme.outline,
                            valueColor: AlwaysStoppedAnimation<Color>(context.theme.colorScheme.primary),
                          );
                        }),
                      ),
                    ),
              backgroundColor: context.theme.colorScheme.properSurface,
              actions: [
                TextButton(
                  onPressed: () {
                    Navigator.of(context).pop();
                  },
                  child: Text(
                    "Cancel", style: context.theme.textTheme.bodyLarge!.copyWith(color: context.theme.colorScheme.primary),
                  ),
                )
              ],
            );
          }).then((_) async {
            myUpdate = null;
            executed.cancel();
            await File(tempPath).delete();
          });
        
        await c.future;

        var returnCode = await executed.getReturnCode();
        if (ReturnCode.isCancel(returnCode)) {
          var output = File("$directory/compressed.mp4");
          if (await output.exists()) {
            await output.delete();
          }
          throw Exception("User cancelled!");
        }
        if (!ReturnCode.isSuccess(returnCode)) {
          var output = await executed.getOutput();
          showSnackbar("Error", "Failed to compress video");
          throw Exception("FFMpeg failed $output code $returnCode");
        } 
        File("$directory/compressed.mp4").renameSync(pathName);
        attachment.totalBytes = File(pathName).lengthSync();
      } else if (dialog) {
        Get.back();
      }
    }
    await c.addMessage(m);
  }

  Future<void> sendAttachment(Chat c, Message m, bool isAudioMessage) async {
    if (m.attachments.isEmpty) return;
    final attachment = m.attachments.first!;
    final progress = attachmentProgress.firstWhere((e) => e.item1 == attachment.guid);
    final completer = Completer<void>();
    latestCancelToken = CancelToken();
    var apnsSuccess = false;
    backend.sendAttachment(
      c, m, isAudioMessage, attachment, onSendProgress: (count, total) => progress.item2.value = count / total,
      cancelToken: latestCancelToken,
    ).then((newMessage) async {
      latestCancelToken = null;

      for (Attachment? a in newMessage.attachments) {
        if (a == null) continue;

        matchAttachmentWithExisting(c, m.guid!, a, existing: attachment)
          .then((_) {
            ms(c.guid).updateMessage(newMessage);
          })
          .catchError((e, stack) {
            Logger.warn("Failed to replace attachment ${a.guid}!", error: e, tag: "AttachmentStatus");
          }
        );
      }

      try {
        await matchMessageWithExisting(c, m.guid!, newMessage, existing: m);
      } catch (e) {
        Logger.warn("Failed to find message match for ${m.guid} -> ${newMessage.guid}!", error: e, tag: "MessageStatus");
      }
      apnsSuccess = true;
      await m.forwardIfNessesary(c);
      attachmentProgress.removeWhere((e) => e.item1 == m.guid || e.item2 >= 1);

      completer.complete();
    }).catchError((error, stack) async {
      latestCancelToken = null;
      Logger.error('Failed to send message!', error: error, trace: stack);

      if (ss.settings.isSmsRouter.value && c.isTextForwarding && !apnsSuccess) {
        // forward to cell even if couldn't send to APNs
        try {
          await m.forwardIfNessesary(c);
          var messageGuid = uuid.v4();
          for (Attachment? a in m.attachments) {
            if (a == null) continue;
            Attachment.replaceAttachment(m.guid, Attachment(
              guid: "${messageGuid}_0",
              uti: a.uti,
              mimeType: a.mimeType,
              isOutgoing: true,
              transferName: a.transferName,
              totalBytes: a.totalBytes,
              hasLivePhoto: a.hasLivePhoto,
              metadata: a.metadata,
              bytes: a.bytes,
            ));
          }
          await Message.replaceMessage(m.guid, m);
          m.guid = messageGuid; // mark it as non-temp
          m.save();
          completer.complete(); // oh well, didn't get to apns, still "sent"
          return;
        } catch(e) {
          error = e;
          Logger.info("Message match failed to forward!", tag: "MessageStatus");
        }
      }

      final tempGuid = m.guid;
      m = handleSendError(error, m);

      if (!ls.isAlive || !(cm.getChatController(c.guid)?.isAlive ?? false)) {
        await notif.createFailedToSend(c);
      }
      await Message.replaceMessage(tempGuid, m);
      attachmentProgress.removeWhere((e) => e.item1 == m.guid || e.item2 >= 1);
      completer.completeError(error);
    });

    return completer.future;
  }

  Future<void> handleNewMessage(Chat c, Message m, String? tempGuid, {bool checkExisting = true}) async {
    Logger.info("handling new ${m.id}");
    // sanity check
    if (checkExisting) {
      final existing = Message.findOne(guid: tempGuid ?? m.guid);
      if (existing != null) {
        Logger.info("handling exsting ${m.id}");
        return await handleUpdatedMessage(c, m, tempGuid, checkExisting: false);
      }
    }
    // should have been handled by the sanity check
    if (tempGuid != null) return;
    Logger.info("New message: [${m.text}] - for chat [${c.guid}]", tag: "ActionHandler");
    // Gets the chat from the db or server (if new)
    c = m.isParticipantEvent ? await handleNewOrUpdatedChat(c) : kIsWeb ? c : (Chat.findOne(guid: c.guid) ?? await handleNewOrUpdatedChat(c));
    // Get the message handle
    m.handle = c.handles.firstWhereOrNull((e) => e.originalROWID == m.handleId) ?? Handle.findOne(originalROWID: m.handleId);
    
    // Display notification if needed and save everything to DB
    bool shouldNotify = shouldNotifyForNewMessageGuid(m.guid!);
    if (!shouldNotify) {
      Logger.info("Not notifying for already handled new message with GUID ${m.guid}...", tag: "ActionHandler");
    }

    if ((!ls.isAlive || ss.settings.endpointUnifiedPush.value != "") && shouldNotify) {
      await MessageHelper.handleNotification(m, c);
    }
    await m.forwardIfNessesary(c, markFailed: true);
    await c.addMessage(m);
  }

  Future<void> handleUpdatedMessage(Chat c, Message m, String? tempGuid, {bool checkExisting = true}) async {
    // sanity check
    if (checkExisting) {
      final existing = Message.findOne(guid: tempGuid ?? m.guid);
      if (existing == null) {
        return await handleNewMessage(c, m, tempGuid, checkExisting: false);
      }
    }
    Logger.info("Updated message: [${m.text}] ${m.getLastUpdate().toLowerCase()} - for chat [${c.guid}]", tag: "ActionHandler");

    // update any attachments
    for (Attachment? a in m.attachments) {
      if (a == null) continue;

      matchAttachmentWithExisting(c, tempGuid ?? m.guid!, a)
        .then((_) {
          ms(c.guid).updateMessage(m);
        })
        .catchError((e, stack) {
          Logger.warn("Failed to replace attachment ${a.guid}!", error: e, trace: stack, tag: "AttachmentStatus");
        }
      );
    }
    await m.forwardIfNessesary(c, markFailed: true); // forward even if retrying
    // update the message in the DB
    await matchMessageWithExisting(c, tempGuid ?? m.guid!, m);
    eventDispatcher.emit("message-updated-${m.guid}");
  }

  Future<Chat> handleNewOrUpdatedChat(Chat partialData) async {
    // fetch all contacts for matching new handles if in background
    if (!ls.isUiThread) {
      await cs.init();
    }
    // get and return the chat from server
    return await cm.fetchChat(partialData.guid) ?? partialData;
  }

  Future<void> handleFaceTimeStatusChange(Map<String, dynamic> data) async {
    if (data["status_id"] == null) return;
    final int statusId = data["status_id"] as int;
    if (statusId == 4) {
      await ActionHandler().handleIncomingFaceTimeCall(data);
    } else if (statusId == 6 && data["uuid"] != null) {
      hideFaceTimeOverlay(data["uuid"]!);
    }
  }

  Future<void> handleIncomingFaceTimeCall(Map<String, dynamic> data) async {
    Logger.info("Handling incoming FaceTime call");
    await cs.init();
    final callUuid = data["uuid"];
    String? address = data["handle"]?["address"];
    String caller = data["address"] ?? "Unknown Number";
    bool isAudio = data["is_audio"];
    Uint8List? chatIcon;

    // Find the contact info for the caller
    // Load the contact's avatar & name
    if (address != null) {
      Contact? contact = cs.getContact(address);
      chatIcon = contact?.avatar;
      caller = contact?.displayName ?? caller;
    }

    if (!ls.isAlive) {
      if (kIsDesktop) {
        await showFaceTimeOverlay(callUuid, caller, chatIcon, isAudio);
      }
      await notif.createIncomingFaceTimeNotification(callUuid, caller, chatIcon, isAudio);
    } else {
      await showFaceTimeOverlay(callUuid, caller, chatIcon, isAudio);
    }
  }

  Future<void> handleIncomingFaceTimeCallLegacy(Map<String, dynamic> data) async {
    Logger.info("Handling incoming FaceTime call (legacy)");
    await cs.init();
    String? address = data["caller"];
    String? caller = address;
    Uint8List? chatIcon;

    // Find the contact info for the caller
    // Load the contact's avatar & name
    if (address != null) {
      Contact? contact = cs.getContact(address);
      chatIcon = contact?.avatar;
      caller = contact?.displayName ?? caller;
      await notif.createIncomingFaceTimeNotification(null, caller!, chatIcon, false);
    }
  }

  Future<void> handleEvent(String event, Map<String, dynamic> data, String source, {bool useQueue = true}) async {
    Logger.info("Received $event from $source");
    switch (event) {
      case "new-message":
        if (!isNullOrEmpty(data)) {
          final payload = ServerPayload.fromJson(data);
          final message = Message.fromMap(payload.data);
          if (message.isFromMe!) {
            if (payload.data['tempGuid'] == null) {
              ah.outOfOrderTempGuids.add(message.guid!);
              await Future.delayed(const Duration(milliseconds: 500));
              if (!ah.outOfOrderTempGuids.contains(message.guid!)) return;
            } else {
              ah.outOfOrderTempGuids.remove(message.guid!);
            }
          }

          IncomingItem item = IncomingItem.fromMap(QueueType.newMessage, payload.data);
          if (useQueue) {
            inq.queue(item);
          } else {
            await ah.handleNewMessage(item.chat, item.message, item.tempGuid);
          }
        }
        return;
      case "updated-message":
        if (!isNullOrEmpty(data)) {
          final payload = ServerPayload.fromJson(data);
          IncomingItem item = IncomingItem.fromMap(QueueType.updatedMessage, payload.data);
          if (useQueue) {
            inq.queue(item);
          } else {
            await ah.handleUpdatedMessage(item.chat, item.message, item.tempGuid);
          }
        }
        return;
      case "group-name-change":
      case "participant-removed":
      case "participant-added":
      case "participant-left":
        try {
          final item = IncomingItem.fromMap(QueueType.updatedMessage, data);
          ah.handleNewOrUpdatedChat(item.chat);
        } catch (_) {}
        return;
      case "chat-read-status-changed":
        Chat? chat = Chat.findOne(guid: data["chatGuid"]);
        if (chat != null && (data["read"] == true || data["read"] == false)) {
          chat.toggleHasUnread(!data["read"]!, privateMark: false);
        }
        return;
      case "typing-indicator":
        final chat = chats.chats.firstWhereOrNull((element) => element.guid == data["guid"]);
        if (chat != null) {
          final controller = cvc(chat);
          controller.showTypingIndicator.value = data["display"];
        }
        return;
      case "incoming-facetime":
        Logger.info("Received legacy incoming FaceTime call");
        await handleIncomingFaceTimeCallLegacy(data);
        return;
      case "ft-call-status-changed":
        Logger.info("Received FaceTime call status change");
        await handleFaceTimeStatusChange(data);
        return;
      case "imessage-aliases-removed":
        Logger.info("Alias(es) removed ${data["aliases"]}");
        await notif.createAliasesRemovedNotification((data["aliases"] as List).cast<String>());
        return;
      default:
        return;
    }
  }
}
