import 'package:bluebubbles/helpers/hex_color.dart';
import 'package:bluebubbles/layouts/setup/pages/page_template.dart';
import 'package:bluebubbles/managers/contact_manager.dart';
import 'package:flutter/material.dart';
import 'package:get/get.dart';
import 'package:permission_handler/permission_handler.dart';

class RequestContacts extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return SetupPageTemplate(
      title: "Contacts Permission",
      subtitle: "We'd like to access your contacts to show contact info in the app.",
      belowSubtitle: FutureBuilder<PermissionStatus>(
        future: Permission.contacts.status,
        initialData: PermissionStatus.denied,
        builder: (context, snapshot) {
          bool granted = snapshot.data! == PermissionStatus.granted;
          return Padding(
            padding: const EdgeInsets.all(8.0),
            child: Align(
              alignment: Alignment.centerLeft,
              child: Text("Permission Status: ${granted ? "Granted" : "Denied"}",
                style: context.theme.textTheme.bodyLarge!.apply(
                  fontSizeDelta: 1.5,
                  color: granted ? Colors.green : context.theme.colorScheme.error,
                ).copyWith(height: 2)),
            ),
          );
        },
      ),
      onNextPressed: () async {
        if (!(await ContactManager().canAccessContacts())) {
          return await showDialog(
            context: context,
            builder: (BuildContext context) {
              return AlertDialog(
                title: Text(
                  "Notice",
                  style: context.theme.textTheme.titleLarge,
                ),
                backgroundColor: context.theme.colorScheme.properSurface,
                content: Padding(
                  padding: const EdgeInsets.all(8.0),
                  child: Text(
                    "We weren't able to access your contacts.\n\nAre you sure you want to proceed without contacts?",
                    style: context.theme.textTheme.bodyLarge),
                ),
                actions: <Widget>[
                  TextButton(
                    child: Text("No", style: context.theme.textTheme.bodyLarge!.copyWith(color: context.theme.colorScheme.primary)),
                    onPressed: () {
                      Navigator.of(context).pop(false);
                    },
                  ),
                  TextButton(
                    child: Text("Yes", style: context.theme.textTheme.bodyLarge!.copyWith(color: context.theme.colorScheme.primary)),
                    onPressed: () {
                      Navigator.of(context).pop(true);
                    },
                  ),
                ],
              );
            },
          );
        } else {
          return true;
        }
      },
    );
  }
}
