import 'package:bluebubbles/services/rustpush/rustpush_service.dart';
import 'package:bluebubbles/services/services.dart';
import 'package:dio/dio.dart';
import 'package:get/get.dart';

BackendService backend = Get.isRegistered<BackendService>() ? Get.find<BackendService>() : Get.put(RustPushBackend());

abstract class BackendService {
  Future<Map<String, dynamic>> createChat(List<String> addresses, String? message, String service, {CancelToken? cancelToken});
  Future<Map<String, dynamic>> sendMessage(String chatGuid, String tempGuid, String message, {String? method, String? effectId, String? subject, String? selectedMessageGuid, int? partIndex, CancelToken? cancelToken});
  Future<bool> renameChat(String chatGuid, String newName);
  Future<bool> chatParticipant(String method, String chatGuid, String newName);
  Future<bool> leaveChat(String chatGuid);
  Future<Map<String, dynamic>> sendTapback(String chatGuid, String selectedText, String selectedGuid, String reaction, int? repPart);
  Future<bool> markRead(String chatGuid);
  HttpService? getRemoteService();
  bool canLeaveChat();
  bool canEditUnsend();
  Future<Map<String, dynamic>?> unsend(String msgGuid, int part);
  Future<Map<String, dynamic>?> edit(String msgGuid, String text, int part);
}