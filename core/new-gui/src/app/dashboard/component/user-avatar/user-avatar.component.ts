import { Component, OnInit, Input } from "@angular/core";

@Component({
  selector: "texera-user-avatar",
  templateUrl: "./user-avatar.component.html",
  styleUrls: ["./user-avatar.component.css"],
})

/**
 * UserAvatarComponent is used to show the avatar of a user
 * The avatar of a Google user will be its Google profile picture
 * The avatar of a normal user will be a default one with the initial
 *
 * @author Zhen Guan
 */
export class UserAvatarComponent implements OnInit {
  constructor() {}

  @Input() imageSrc?: string;

  @Input() userName?: string;

  ngOnInit(): void {
    if (!this.imageSrc && !this.userName) {
      throw new Error("image source or user name should be provided");
    }
  }

  public getUserInitial(userName: string): string {
    return userName
      .split(" ")
      .map(n => n[0])
      .join("");
  }
}
